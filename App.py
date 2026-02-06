import json
import os
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


APP_NAME = "Shortsheet Builder (SO-tied Shipped)"
CONFIG_FILE = "config.json"


# -----------------------------
# Config
# -----------------------------
@dataclass
class AppConfig:
    server: str = ""
    database: str = "WPL"
    driver: str = ""
    auth_mode: str = "windows"  # windows / sql
    username: str = ""
    password: str = ""
    product_excel_path: str = "Product Info.xlsx"
    product_sheet_name: str = ""
    output_folder: str = ""


def load_config() -> AppConfig:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return AppConfig(**{**AppConfig().__dict__, **data})
        except Exception:
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
        preferred, other = [], []
        for d in drivers:
            if "SQL Server" in d or "ODBC Driver" in d:
                preferred.append(d)
            else:
                other.append(d)
        return preferred[::-1] + other
    except Exception:
        return []


def build_connection_string(cfg: AppConfig) -> str:
    if not cfg.driver:
        raise ValueError("ODBC Driver is blank.")
    if not cfg.server:
        raise ValueError("Server Address is blank.")

    base = f"DRIVER={{{cfg.driver}}};SERVER={cfg.server};DATABASE={cfg.database};"

    if cfg.auth_mode == "windows":
        return base + "Trusted_Connection=yes;"
    else:
        if not cfg.username:
            raise ValueError("SQL username is blank.")
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


# -----------------------------
# Product master helpers
# -----------------------------
def load_product_master(path: str, sheet_name: str = "") -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()

    if sheet_name.strip():
        df = pd.read_excel(path, sheet_name=sheet_name.strip())
    else:
        df = pd.read_excel(path)

    df.columns = [str(c).strip() for c in df.columns]
    return df


def infer_master_cols(df: pd.DataFrame) -> dict:
    cols = {c.lower(): c for c in df.columns}
    plu_candidates = ["plu", "productnumber", "product_number", "itemnumber", "item_number", "code"]
    desc_candidates = ["product description", "productdescription", "description", "desc", "name"]

    def find_any(cands):
        for k in cands:
            if k in cols:
                return cols[k]
        for c in df.columns:
            cl = c.lower()
            for k in cands:
                if k in cl:
                    return c
        return ""

    return {"plu": find_any(plu_candidates), "desc": find_any(desc_candidates)}


def merge_description(short_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    if master_df.empty:
        return short_df

    colmap = infer_master_cols(master_df)
    if not colmap["plu"]:
        return short_df

    m = master_df.copy()
    m[colmap["plu"]] = m[colmap["plu"]].astype(str).str.strip().str.zfill(5)

    keep = [colmap["plu"]]
    if colmap["desc"]:
        keep.append(colmap["desc"])

    m2 = m[keep].drop_duplicates(subset=[colmap["plu"]])

    out = short_df.copy()
    out["PLU"] = out["PLU"].astype(str).str.strip().str.zfill(5)

    merged = out.merge(m2, how="left", left_on="PLU", right_on=colmap["plu"])
    if colmap["plu"] in merged.columns:
        merged = merged.drop(columns=[colmap["plu"]])
    if colmap["desc"] and colmap["desc"] in merged.columns:
        merged = merged.rename(columns={colmap["desc"]: "ProductDescription"})

    desired = ["PLU", "ProductDescription", "QtyOrdered", "QtyShipped", "AvailableCases", "RemainingCases"]
    cols = [c for c in desired if c in merged.columns] + [c for c in merged.columns if c not in desired]
    return merged[cols]


# -----------------------------
# Core query (SO-tied shipped)
# -----------------------------
def fetch_shortsheets(
    conn,
    schedule_date: date,
    so_date_field: str,
    include_available_statuses: list[str],
    only_remaining: bool = True
) -> pd.DataFrame:
    """
    Builds shortsheet by PLU for SOs in a schedule day:
      - Ordered from SO_Detail (Quantity) by PLU (ItemRef_FullName like '00269')
      - Shipped from Shipped (QtyShipped) ONLY for those SOs (OrderNum in OrdersForDay)
      - Available from Wip filtered to PLUs on those orders, using selected statuses
      - Remaining = Ordered - Shipped
    """

    allowed_fields = {"TxnDate", "ShipDate", "DueDate", "TimeCreated"}
    if so_date_field not in allowed_fields:
        raise ValueError(f"Invalid SO date field: {so_date_field}")

    # Build safe IN (...) list for statuses
    statuses = [s.strip() for s in include_available_statuses if s.strip()]
    if not statuses:
        statuses = ["Available"]

    # NOTE: status literals are safe since we control list, but still escape single quotes just in case
    status_sql = ", ".join([f"'{s.replace(\"'\", \"''\")}'" for s in statuses])

    where_remaining = "WHERE (o.QtyOrdered - COALESCE(s.QtyShipped,0)) > 0" if only_remaining else ""

    date_col = f"so.{so_date_field}"

    sql = f"""
    WITH OrdersForDay AS (
        SELECT so.TxnID
        FROM WPL.dbo.GP_SalesOrder so
        WHERE {date_col} >= CAST(? AS datetime2(0))
          AND {date_col} <  DATEADD(day, 1, CAST(? AS datetime2(0)))
    ),
    OrderedAgg AS (
        SELECT
            TRY_CONVERT(int, d.ItemRef_FullName) AS PLU_Int,
            SUM(COALESCE(d.Quantity, 0)) AS QtyOrdered
        FROM WPL.dbo.GP_SalesOrderLineDetail d
        JOIN OrdersForDay o
          ON o.TxnID = d.TxnIDKey
        WHERE TRY_CONVERT(int, d.ItemRef_FullName) IS NOT NULL
        GROUP BY TRY_CONVERT(int, d.ItemRef_FullName)
    ),
    ShippedAgg AS (
        SELECT
            TRY_CONVERT(int, s.ProductNumber) AS PLU_Int,
            SUM(COALESCE(s.QtyShipped, 0)) AS QtyShipped
        FROM WPL.dbo.Shipped s
        JOIN OrdersForDay o
          ON o.TxnID = s.OrderNum
        WHERE TRY_CONVERT(int, s.ProductNumber) IS NOT NULL
        GROUP BY TRY_CONVERT(int, s.ProductNumber)
    ),
    InvAgg AS (
        SELECT
            TRY_CONVERT(int, w.plu) AS PLU_Int,
            COUNT(*) AS AvailableCases
        FROM WPL.dbo.Wip w
        -- Only pull WIP for PLUs that are on today's orders (keeps it light)
        JOIN OrderedAgg oa
          ON oa.PLU_Int = TRY_CONVERT(int, w.plu)
        WHERE w.status IN ({status_sql})
          AND TRY_CONVERT(int, w.plu) IS NOT NULL
        GROUP BY TRY_CONVERT(int, w.plu)
    )
    SELECT
        oa.PLU_Int,
        oa.QtyOrdered,
        COALESCE(s.QtyShipped, 0) AS QtyShipped,
        COALESCE(i.AvailableCases, 0) AS AvailableCases,
        (oa.QtyOrdered - COALESCE(s.QtyShipped, 0)) AS RemainingCases
    FROM OrderedAgg oa
    LEFT JOIN ShippedAgg s
      ON s.PLU_Int = oa.PLU_Int
    LEFT JOIN InvAgg i
      ON i.PLU_Int = oa.PLU_Int
    {where_remaining}
    ORDER BY RemainingCases DESC, oa.PLU_Int;
    """

    params = (schedule_date.isoformat(), schedule_date.isoformat())
    df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return df

    # Format PLU as 5 digits like '00269'
    df["PLU"] = df["PLU_Int"].astype(int).astype(str).str.zfill(5)
    df = df.drop(columns=["PLU_Int"])
    return df


def export_to_excel(df: pd.DataFrame, out_path: str) -> None:
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Shortsheet", index=False)


# -----------------------------
# GUI
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("980x680")

        self.cfg = load_config()

        # connection vars
        self.var_server = tk.StringVar(value=self.cfg.server)
        self.var_database = tk.StringVar(value=self.cfg.database)
        self.var_driver = tk.StringVar(value=self.cfg.driver)
        self.var_auth = tk.StringVar(value=self.cfg.auth_mode)
        self.var_user = tk.StringVar(value=self.cfg.username)
        self.var_pass = tk.StringVar(value=self.cfg.password)

        # product excel vars
        self.var_product_path = tk.StringVar(value=self.cfg.product_excel_path)
        self.var_product_sheet = tk.StringVar(value=self.cfg.product_sheet_name)

        # run vars
        self.var_schedule_date = tk.StringVar(value=date.today().isoformat())
        self.var_so_date_field = tk.StringVar(value="ShipDate")  # default because that's typically the "schedule"
        self.var_only_remaining = tk.BooleanVar(value=True)

        # WIP statuses for AvailableCases (configurable)
        self.var_wip_available = tk.BooleanVar(value=True)
        self.var_wip_scanning = tk.BooleanVar(value=True)
        self.var_wip_waiting = tk.BooleanVar(value=True)

        self.var_output_folder = tk.StringVar(value=self.cfg.output_folder or os.getcwd())

        self._build_ui()
        self._refresh_auth_state()
        self._log("Ready.")

    def _build_ui(self):
        pad = {"padx": 10, "pady": 6}

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
        ttk.Combobox(frm_conn, textvariable=self.var_driver, values=drivers, width=42).grid(row=row, column=1, sticky="w", **pad)
        if drivers and not self.var_driver.get():
            self.var_driver.set(drivers[0])

        ttk.Label(frm_conn, text="Auth Mode:").grid(row=row, column=2, sticky="w", **pad)
        cmb_auth = ttk.Combobox(frm_conn, textvariable=self.var_auth, values=["windows", "sql"], width=15, state="readonly")
        cmb_auth.grid(row=row, column=3, sticky="w", **pad)
        cmb_auth.bind("<<ComboboxSelected>>", lambda e: self._refresh_auth_state())

        row += 1
        ttk.Label(frm_conn, text="Username:").grid(row=row, column=0, sticky="w", **pad)
        self.ent_user = ttk.Entry(frm_conn, textvariable=self.var_user, width=45)
        self.ent_user.grid(row=row, column=1, sticky="w", **pad)

        ttk.Label(frm_conn, text="Password:").grid(row=row, column=2, sticky="w", **pad)
        self.ent_pass = ttk.Entry(frm_conn, textvariable=self.var_pass, show="*", width=18)
        self.ent_pass.grid(row=row, column=3, sticky="w", **pad)

        row += 1
        ttk.Button(frm_conn, text="Test Connection", command=self.on_test_connection).grid(row=row, column=0, sticky="w", **pad)
        ttk.Button(frm_conn, text="Save Settings", command=self.on_save_settings).grid(row=row, column=1, sticky="w", **pad)

        frm_prod = ttk.LabelFrame(self, text="Product Master Excel (optional: add description)")
        frm_prod.pack(fill="x", **pad)

        row = 0
        ttk.Label(frm_prod, text="Excel File Path:").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_prod, textvariable=self.var_product_path, width=74).grid(row=row, column=1, sticky="w", **pad)
        ttk.Button(frm_prod, text="Browse...", command=self.on_browse_product).grid(row=row, column=2, sticky="w", **pad)

        row += 1
        ttk.Label(frm_prod, text="Sheet Name (optional):").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_prod, textvariable=self.var_product_sheet, width=30).grid(row=row, column=1, sticky="w", **pad)

        frm_run = ttk.LabelFrame(self, text="Shortsheet Run")
        frm_run.pack(fill="x", **pad)

        row = 0
        ttk.Label(frm_run, text="Schedule Date (YYYY-MM-DD):").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_run, textvariable=self.var_schedule_date, width=18).grid(row=row, column=1, sticky="w", **pad)

        ttk.Label(frm_run, text="SO Date Field:").grid(row=row, column=2, sticky="w", **pad)
        ttk.Combobox(frm_run, textvariable=self.var_so_date_field,
                     values=["TxnDate", "ShipDate", "DueDate", "TimeCreated"],
                     state="readonly", width=16).grid(row=row, column=3, sticky="w", **pad)

        ttk.Checkbutton(frm_run, text="Only Remaining (>0)", variable=self.var_only_remaining).grid(row=row, column=4, sticky="w", **pad)

        row += 1
        ttk.Label(frm_run, text="WIP statuses to count as Available:").grid(row=row, column=0, sticky="w", **pad)
        ttk.Checkbutton(frm_run, text="Available", variable=self.var_wip_available).grid(row=row, column=1, sticky="w", **pad)
        ttk.Checkbutton(frm_run, text="ScanningSalesOrder", variable=self.var_wip_scanning).grid(row=row, column=2, sticky="w", **pad)
        ttk.Checkbutton(frm_run, text="WaitingToBeInvoiced", variable=self.var_wip_waiting).grid(row=row, column=3, sticky="w", **pad)

        row += 1
        ttk.Label(frm_run, text="Output Folder:").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_run, textvariable=self.var_output_folder, width=50).grid(row=row, column=1, sticky="w", **pad)
        ttk.Button(frm_run, text="Browse...", command=self.on_browse_output).grid(row=row, column=2, sticky="w", **pad)

        ttk.Button(frm_run, text="Build Shortsheet", command=self.on_build).grid(row=row, column=3, sticky="w", **pad)

        frm_log = ttk.LabelFrame(self, text="Log")
        frm_log.pack(fill="both", expand=True, **pad)

        self.txt_log = tk.Text(frm_log, height=16, wrap="word")
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=10)

    def _refresh_auth_state(self):
        is_sql = self.var_auth.get().strip().lower() == "sql"
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

    def _parse_date(self, s: str) -> date:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()

    def _connect(self, cfg: AppConfig):
        return pyodbc.connect(build_connection_string(cfg), timeout=20)

    def on_save_settings(self):
        self.cfg = self._collect_config()
        save_config(self.cfg)
        self._log("Settings saved to config.json")

    def on_test_connection(self):
        cfg = self._collect_config()
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

    def on_build(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed. Run: pip install pyodbc")
            return

        cfg = self._collect_config()

        try:
            sched = self._parse_date(self.var_schedule_date.get())
        except ValueError:
            messagebox.showerror("Invalid Date", "Enter date as YYYY-MM-DD (example: 2026-02-06).")
            return

        statuses = []
        if self.var_wip_available.get():
            statuses.append("Available")
        if self.var_wip_scanning.get():
            statuses.append("ScanningSalesOrder")
        if self.var_wip_waiting.get():
            statuses.append("WaitingToBeInvoiced")

        only_remaining = bool(self.var_only_remaining.get())
        so_field = self.var_so_date_field.get().strip()

        # Load master (optional)
        master_df = load_product_master(cfg.product_excel_path, cfg.product_sheet_name)
        if not master_df.empty:
            self._log(f"Loaded product master rows: {len(master_df):,}")
        else:
            self._log("No product master loaded (descriptions will be blank).")

        try:
            self._log("Connecting to SQL Server...")
            with self._connect(cfg) as conn:
                self._log(f"Running shortsheet for {sched.isoformat()} using SO.{so_field} ...")
                df = fetch_shortsheets(
                    conn,
                    schedule_date=sched,
                    so_date_field=so_field,
                    include_available_statuses=statuses,
                    only_remaining=only_remaining
                )
            self._log(f"Rows returned: {len(df):,}")
        except Exception as e:
            self._log("ERROR running query:")
            self._log(str(e))
            self._log(traceback.format_exc())
            messagebox.showerror("SQL Error", str(e))
            return

        if df.empty:
            self._log("No rows returned. Try changing SO Date Field (TxnDate vs ShipDate).")
            messagebox.showinfo("No Results", "No rows returned. Try changing the SO Date Field.")
            return

        # Merge description (optional)
        try:
            df = merge_description(df, master_df) if not master_df.empty else df
        except Exception as e:
            self._log(f"WARNING: description merge failed: {e}")

        # Export
        try:
            out_folder = cfg.output_folder or os.getcwd()
            os.makedirs(out_folder, exist_ok=True)
            out_path = os.path.join(out_folder, f"Shortsheet_{so_field}_{sched.isoformat()}.xlsx")
            self._log(f"Exporting: {out_path}")
            export_to_excel(df, out_path)
            self._log("Export complete.")
            messagebox.showinfo("Done", f"Shortsheet created:\n{out_path}")
        except Exception as e:
            self._log(f"ERROR exporting: {e}")
            messagebox.showerror("Export Error", str(e))


def main():
    App().mainloop()


if __name__ == "__main__":
    main()
