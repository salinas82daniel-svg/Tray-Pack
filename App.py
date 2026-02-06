import json
import os
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, date
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import pandas as pd

try:
    import pyodbc
except ImportError:
    pyodbc = None


APP_NAME = "SQL Shortsheet Builder"
CONFIG_FILE = "config.json"


# -----------------------------
# Config model
# -----------------------------
@dataclass
class AppConfig:
    server: str = ""
    database: str = "WPL"
    driver: str = ""
    auth_mode: str = "windows"  # "windows" or "sql"
    username: str = ""
    password: str = ""
    product_excel_path: str = "Product Info.xlsx"
    product_sheet_name: str = ""  # blank = first sheet
    output_folder: str = ""


def load_config() -> AppConfig:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return AppConfig(**{**AppConfig().__dict__, **data})
        except Exception:
            # If config is corrupt, start fresh
            return AppConfig()
    return AppConfig()


def save_config(cfg: AppConfig) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg.__dict__, f, indent=2)


# -----------------------------
# SQL helpers
# -----------------------------
def list_odbc_drivers() -> list[str]:
    if pyodbc is None:
        return []
    try:
        drivers = pyodbc.drivers()
        # Favor common SQL Server drivers at top
        preferred = []
        other = []
        for d in drivers:
            if "SQL Server" in d or "ODBC Driver" in d:
                preferred.append(d)
            else:
                other.append(d)
        return preferred[::-1] + other  # reverse preferred so newest typically higher (often)
    except Exception:
        return []


def build_connection_string(cfg: AppConfig) -> str:
    # Example drivers:
    # - "ODBC Driver 18 for SQL Server"
    # - "ODBC Driver 17 for SQL Server"
    # - "SQL Server"
    if not cfg.driver:
        raise ValueError("ODBC Driver is blank. Select an installed SQL Server ODBC driver.")

    base = f"DRIVER={{{cfg.driver}}};SERVER={cfg.server};DATABASE={cfg.database};"

    if cfg.auth_mode == "windows":
        # Trusted Connection
        return base + "Trusted_Connection=yes;"
    else:
        if not cfg.username:
            raise ValueError("SQL username is blank.")
        # Note: For ODBC Driver 18, encryption defaults may require extra params depending on your environment.
        # If you run into SSL/Encrypt errors, we can add: Encrypt=yes;TrustServerCertificate=yes;
        return base + f"UID={cfg.username};PWD={cfg.password};"


def test_connection(cfg: AppConfig) -> tuple[bool, str]:
    if pyodbc is None:
        return False, "pyodbc is not installed. Run: pip install pyodbc"
    try:
        cs = build_connection_string(cfg)
        with pyodbc.connect(cs, timeout=5) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
        return True, "Connection successful."
    except Exception as e:
        return False, f"Connection failed:\n{e}"


def fetch_shortsheets(conn, report_date: date) -> pd.DataFrame:
    # Uses your earlier logic:
    # - Orders for day from GP_SalesOrder.TxnDate
    # - Shipped: aggregate by ProductNumber
    # - Wip: available cases by plu for those SOs
    #
    # NOTE: If Wip rows are not 1 row per case, we can change COUNT(*) to SUM(field).
    sql = """
    WITH OrdersForDay AS (
        SELECT so.TxnID
        FROM WPL.dbo.GP_SalesOrder AS so
        WHERE so.TxnDate >= CAST(? AS datetime2(0))
          AND so.TxnDate <  DATEADD(day, 1, CAST(? AS datetime2(0)))
    ),
    ShippedAgg AS (
        SELECT
            s.ProductNumber AS PLU,
            SUM(COALESCE(s.QtyOrdered, 0)) AS QtyOrdered,
            SUM(COALESCE(s.QtyShipped, 0)) AS QtyShipped
        FROM WPL.dbo.Shipped AS s
        JOIN OrdersForDay o
          ON o.TxnID = s.OrderNum
        GROUP BY s.ProductNumber
    ),
    InvAgg AS (
        SELECT
            w.plu AS PLU,
            COUNT(*) AS AvailableCases
        FROM WPL.dbo.Wip AS w
        JOIN OrdersForDay o
          ON o.TxnID = w.SO
        WHERE w.status IN ('Available','ScanningSalesOrder','WaitingToBeInvoiced')
        GROUP BY w.plu
    )
    SELECT
        sa.PLU,
        sa.QtyOrdered,
        sa.QtyShipped,
        COALESCE(ia.AvailableCases, 0) AS AvailableCases,
        (sa.QtyOrdered - sa.QtyShipped) AS RemainingCases
    FROM ShippedAgg sa
    LEFT JOIN InvAgg ia
      ON ia.PLU = sa.PLU
    WHERE (sa.QtyOrdered - sa.QtyShipped) > 0
    ORDER BY RemainingCases DESC, sa.PLU;
    """
    # Parameterize date safely
    params = (report_date.isoformat(), report_date.isoformat())
    df = pd.read_sql(sql, conn, params=params)
    return df


# -----------------------------
# Product master helpers
# -----------------------------
def load_product_master(path: str, sheet_name: str = "") -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Product Excel not found: {path}")

    # Let pandas choose engine; openpyxl is common
    if sheet_name.strip():
        df = pd.read_excel(path, sheet_name=sheet_name.strip())
    else:
        df = pd.read_excel(path)

    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]

    return df


def infer_product_columns(df: pd.DataFrame) -> dict:
    """
    Try to guess likely columns.
    You can hard-map later once you confirm your master file columns.
    """
    cols = {c.lower(): c for c in df.columns}

    # Common candidates
    plu_candidates = ["plu", "productnumber", "product_number", "code", "item", "itemnumber"]
    desc_candidates = ["description", "product description", "productdescription", "desc", "itemref_fullname", "name"]
    tpc_candidates = ["trays per case", "trayspercase", "trays_case", "trays/case", "tpc"]
    tpm_candidates = ["standard trays per minute", "stdtpm", "tpm", "trays per minute", "std trays per minute"]

    def find_any(cands):
        for k in cands:
            if k in cols:
                return cols[k]
        # also try contains match
        for c in df.columns:
            cl = c.lower()
            for k in cands:
                if k in cl:
                    return c
        return ""

    return {
        "plu": find_any(plu_candidates),
        "description": find_any(desc_candidates),
        "trays_per_case": find_any(tpc_candidates),
        "std_tpm": find_any(tpm_candidates),
    }


def merge_with_master(short_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    colmap = infer_product_columns(master_df)

    if not colmap["plu"]:
        raise ValueError(
            "Couldn't detect the PLU column in your Product Excel.\n"
            "Make sure your product file has a column named like: PLU, ProductNumber, ItemNumber, etc."
        )

    # Prepare PLU keys as strings for safe join
    s = short_df.copy()
    m = master_df.copy()

    s["PLU"] = s["PLU"].astype(str).str.strip()
    m[colmap["plu"]] = m[colmap["plu"]].astype(str).str.strip()

    keep_cols = [colmap["plu"]]
    if colmap["description"]:
        keep_cols.append(colmap["description"])
    if colmap["trays_per_case"]:
        keep_cols.append(colmap["trays_per_case"])
    if colmap["std_tpm"]:
        keep_cols.append(colmap["std_tpm"])

    m2 = m[keep_cols].drop_duplicates(subset=[colmap["plu"]])

    merged = s.merge(
        m2,
        how="left",
        left_on="PLU",
        right_on=colmap["plu"],
        suffixes=("", "_master"),
    )

    # Rename master columns to desired output headers
    rename_map = {}
    if colmap["description"]:
        rename_map[colmap["description"]] = "ProductDescription"
    if colmap["trays_per_case"]:
        rename_map[colmap["trays_per_case"]] = "TraysPerCase"
    if colmap["std_tpm"]:
        rename_map[colmap["std_tpm"]] = "StdTraysPerMin"

    merged = merged.rename(columns=rename_map)

    # Remove duplicate PLU column from master
    if colmap["plu"] in merged.columns:
        merged = merged.drop(columns=[colmap["plu"]])

    # Arrange columns
    desired = ["PLU", "ProductDescription", "TraysPerCase", "StdTraysPerMin",
               "QtyOrdered", "QtyShipped", "AvailableCases", "RemainingCases"]
    cols = [c for c in desired if c in merged.columns] + [c for c in merged.columns if c not in desired]
    merged = merged[cols]

    return merged


def export_to_excel(df: pd.DataFrame, out_path: str) -> None:
    # Using openpyxl via pandas ExcelWriter
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Shortsheet", index=False)


# -----------------------------
# GUI
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("860x560")

        self.cfg = load_config()

        # Variables
        self.var_server = tk.StringVar(value=self.cfg.server)
        self.var_database = tk.StringVar(value=self.cfg.database)
        self.var_driver = tk.StringVar(value=self.cfg.driver)
        self.var_auth = tk.StringVar(value=self.cfg.auth_mode)
        self.var_user = tk.StringVar(value=self.cfg.username)
        self.var_pass = tk.StringVar(value=self.cfg.password)
        self.var_product_path = tk.StringVar(value=self.cfg.product_excel_path)
        self.var_product_sheet = tk.StringVar(value=self.cfg.product_sheet_name)

        today = date.today().isoformat()
        self.var_report_date = tk.StringVar(value=today)

        self.var_output_folder = tk.StringVar(value=self.cfg.output_folder or os.getcwd())

        self._build_ui()
        self._refresh_auth_state()

    def _build_ui(self):
        pad = {"padx": 10, "pady": 6}

        # Connection frame
        frm_conn = ttk.LabelFrame(self, text="SQL Connection")
        frm_conn.pack(fill="x", **pad)

        row = 0
        ttk.Label(frm_conn, text="Server Address:").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_conn, textvariable=self.var_server, width=45).grid(row=row, column=1, sticky="w", **pad)

        ttk.Label(frm_conn, text="Database:").grid(row=row, column=2, sticky="w", **pad)
        ttk.Entry(frm_conn, textvariable=self.var_database, width=18).grid(row=row, column=3, sticky="w", **pad)

        row += 1
        ttk.Label(frm_conn, text="ODBC Driver:").grid(row=row, column=0, sticky="w", **pad)
        drivers = list_odbc_drivers()
        self.cmb_driver = ttk.Combobox(frm_conn, textvariable=self.var_driver, values=drivers, width=42)
        self.cmb_driver.grid(row=row, column=1, sticky="w", **pad)
        if drivers and not self.var_driver.get():
            self.var_driver.set(drivers[0])

        ttk.Label(frm_conn, text="Auth Mode:").grid(row=row, column=2, sticky="w", **pad)
        self.cmb_auth = ttk.Combobox(frm_conn, textvariable=self.var_auth, values=["windows", "sql"], width=15, state="readonly")
        self.cmb_auth.grid(row=row, column=3, sticky="w", **pad)
        self.cmb_auth.bind("<<ComboboxSelected>>", lambda e: self._refresh_auth_state())

        row += 1
        ttk.Label(frm_conn, text="Username:").grid(row=row, column=0, sticky="w", **pad)
        self.ent_user = ttk.Entry(frm_conn, textvariable=self.var_user, width=45)
        self.ent_user.grid(row=row, column=1, sticky="w", **pad)

        ttk.Label(frm_conn, text="Password:").grid(row=row, column=2, sticky="w", **pad)
        self.ent_pass = ttk.Entry(frm_conn, textvariable=self.var_pass, show="*", width=18)
        self.ent_pass.grid(row=row, column=3, sticky="w", **pad)

        row += 1
        btn_test = ttk.Button(frm_conn, text="Test Connection", command=self.on_test_connection)
        btn_test.grid(row=row, column=0, sticky="w", **pad)

        btn_save = ttk.Button(frm_conn, text="Save Settings", command=self.on_save_settings)
        btn_save.grid(row=row, column=1, sticky="w", **pad)

        # Product master frame
        frm_prod = ttk.LabelFrame(self, text="Product Master Excel (PLU database)")
        frm_prod.pack(fill="x", **pad)

        row = 0
        ttk.Label(frm_prod, text="Excel File Path:").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_prod, textvariable=self.var_product_path, width=70).grid(row=row, column=1, sticky="w", **pad)
        ttk.Button(frm_prod, text="Browse...", command=self.on_browse_product).grid(row=row, column=2, sticky="w", **pad)

        row += 1
        ttk.Label(frm_prod, text="Sheet Name (optional):").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_prod, textvariable=self.var_product_sheet, width=30).grid(row=row, column=1, sticky="w", **pad)

        # Run frame
        frm_run = ttk.LabelFrame(self, text="Shortsheet Run")
        frm_run.pack(fill="x", **pad)

        row = 0
        ttk.Label(frm_run, text="Sales Date (YYYY-MM-DD):").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_run, textvariable=self.var_report_date, width=18).grid(row=row, column=1, sticky="w", **pad)

        ttk.Label(frm_run, text="Output Folder:").grid(row=row, column=2, sticky="w", **pad)
        ttk.Entry(frm_run, textvariable=self.var_output_folder, width=30).grid(row=row, column=3, sticky="w", **pad)
        ttk.Button(frm_run, text="Browse...", command=self.on_browse_output).grid(row=row, column=4, sticky="w", **pad)

        row += 1
        ttk.Button(frm_run, text="Build Shortsheet", command=self.on_build_shortsheets).grid(row=row, column=0, sticky="w", **pad)

        # Log box
        frm_log = ttk.LabelFrame(self, text="Log")
        frm_log.pack(fill="both", expand=True, **pad)

        self.txt_log = tk.Text(frm_log, height=12, wrap="word")
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=10)

        self._log("Ready.")

    def _refresh_auth_state(self):
        mode = self.var_auth.get().strip().lower()
        is_sql = mode == "sql"
        state = "normal" if is_sql else "disabled"
        self.ent_user.configure(state=state)
        self.ent_pass.configure(state=state)

    def _collect_config(self) -> AppConfig:
        return AppConfig(
            server=self.var_server.get().strip(),
            database=self.var_database.get().strip() or "WPL",
            driver=self.var_driver.get().strip(),
            auth_mode=self.var_auth.get().strip().lower(),
            username=self.var_user.get().strip(),
            password=self.var_pass.get(),
            product_excel_path=self.var_product_path.get().strip(),
            product_sheet_name=self.var_product_sheet.get().strip(),
            output_folder=self.var_output_folder.get().strip(),
        )

    def _log(self, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.txt_log.insert("end", f"[{ts}] {msg}\n")
        self.txt_log.see("end")
        self.update_idletasks()

    def on_save_settings(self):
        self.cfg = self._collect_config()
        save_config(self.cfg)
        self._log("Settings saved to config.json")

    def on_test_connection(self):
        cfg = self._collect_config()
        if not cfg.server:
            messagebox.showerror("Missing Server", "Please enter SQL Server address.")
            return

        ok, msg = test_connection(cfg)
        self._log(msg)
        if ok:
            messagebox.showinfo("Test Connection", msg)
        else:
            messagebox.showerror("Test Connection", msg)

    def on_browse_product(self):
        path = filedialog.askopenfilename(
            title="Select Product Master Excel",
            filetypes=[("Excel Files", "*.xlsx *.xls"), ("All Files", "*.*")]
        )
        if path:
            self.var_product_path.set(path)

    def on_browse_output(self):
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.var_output_folder.set(folder)

    def on_build_shortsheets(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed. Run: pip install pyodbc")
            return

        cfg = self._collect_config()
        if not cfg.server:
            messagebox.showerror("Missing Server", "Please enter SQL Server address.")
            return

        # Parse date
        try:
            rpt = datetime.strptime(self.var_report_date.get().strip(), "%Y-%m-%d").date()
        except ValueError:
            messagebox.showerror("Invalid Date", "Enter date as YYYY-MM-DD (example: 2026-02-05).")
            return

        # Load product master
        try:
            self._log(f"Loading product master: {cfg.product_excel_path}")
            master_df = load_product_master(cfg.product_excel_path, cfg.product_sheet_name)
            self._log(f"Loaded product master rows: {len(master_df):,}")
        except Exception as e:
            self._log(f"ERROR loading product master: {e}")
            messagebox.showerror("Product Master Error", str(e))
            return

        # Connect and fetch
        try:
            cs = build_connection_string(cfg)
            self._log("Connecting to SQL Server...")
            with pyodbc.connect(cs, timeout=15) as conn:
                self._log(f"Fetching shortsheet data for {rpt.isoformat()} ...")
                short_df = fetch_shortsheets(conn, rpt)
            self._log(f"Shortsheet rows returned: {len(short_df):,}")
        except Exception as e:
            self._log("ERROR fetching from SQL:")
            self._log(str(e))
            self._log(traceback.format_exc())
            messagebox.showerror("SQL Error", str(e))
            return

        if short_df.empty:
            self._log("No remaining balances found (query returned 0 rows).")
            messagebox.showinfo("No Results", "No remaining balances found for that date.")
            return

        # Merge
        try:
            self._log("Merging with product master (by PLU)...")
            merged = merge_with_master(short_df, master_df)
            self._log("Merge complete.")
        except Exception as e:
            self._log(f"ERROR merging: {e}")
            messagebox.showerror("Merge Error", str(e))
            return

        # Export
        try:
            out_folder = cfg.output_folder or os.getcwd()
            os.makedirs(out_folder, exist_ok=True)
            out_path = os.path.join(out_folder, f"Shortsheet_{rpt.isoformat()}.xlsx")
            self._log(f"Exporting to: {out_path}")
            export_to_excel(merged, out_path)
            self._log("Export complete.")
            messagebox.showinfo("Done", f"Shortsheet created:\n{out_path}")
        except Exception as e:
            self._log(f"ERROR exporting: {e}")
            messagebox.showerror("Export Error", str(e))


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
