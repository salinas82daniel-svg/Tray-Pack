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


APP_NAME = "Shortsheet Builder"
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
    product_sheet_name: str = ""
    output_folder: str = ""


def load_config() -> AppConfig:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            merged = {**AppConfig().__dict__, **data}
            return AppConfig(**merged)
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
        # Prefer SQL Server drivers first
        preferred = []
        other = []
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
        raise ValueError("ODBC Driver is blank. Select an installed SQL Server driver.")
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
        with pyodbc.connect(cs, timeout=8) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
        return True, "Connection successful."
    except Exception as e:
        return False, f"{e}"


def _escape_sql_literal(s: str) -> str:
    # escape single quotes for SQL
    return s.replace("'", "''")


def _build_in_list_sql(values: list[str]) -> str:
    """
    Returns:  'A','B','C'
    Safe for our controlled checkbox values.
    PyInstaller-safe (no f-string backslash issues).
    """
    escaped = []
    for v in values:
        escaped.append("'" + _escape_sql_literal(v) + "'")
    return ", ".join(escaped)


# -----------------------------
# Queries
# -----------------------------
def run_diagnostics(conn, schedule_date: date, so_date_field: str, statuses: list[str]) -> dict:
    allowed = {"TxnDate", "ShipDate", "DueDate", "TimeCreated"}
    if so_date_field not in allowed:
        raise ValueError(f"Invalid SO date field '{so_date_field}'")

    status_sql = _build_in_list_sql(statuses if statuses else ["Available"])

    sql_counts = f"""
    WITH OrdersForDay AS (
        SELECT so.TxnID
        FROM WPL.dbo.GP_SalesOrder so
        WHERE so.{so_date_field} >= CAST(? AS datetime2(0))
          AND so.{so_date_field} <  DATEADD(day, 1, CAST(? AS datetime2(0)))
    ),
    OrderedAgg AS (
        SELECT
            TRY_CONVERT(int, d.ItemRef_FullName) AS PLU_Int,
            SUM(COALESCE(d.Quantity,0)) AS QtyOrdered
        FROM WPL.dbo.GP_SalesOrderLineDetail d
        JOIN OrdersForDay o ON o.TxnID = d.TxnIDKey
        WHERE TRY_CONVERT(int, d.ItemRef_FullName) IS NOT NULL
        GROUP BY TRY_CONVERT(int, d.ItemRef_FullName)
    ),
    ShippedAgg AS (
        SELECT
            TRY_CONVERT(int, s.ProductNumber) AS PLU_Int,
            SUM(COALESCE(s.QtyShipped,0)) AS QtyShipped
        FROM WPL.dbo.Shipped s
        JOIN OrdersForDay o ON o.TxnID = s.OrderNum
        WHERE TRY_CONVERT(int, s.ProductNumber) IS NOT NULL
        GROUP BY TRY_CONVERT(int, s.ProductNumber)
    ),
    InvAgg AS (
        SELECT
            TRY_CONVERT(int, w.plu) AS PLU_Int,
            COUNT(*) AS AvailableCases
        FROM WPL.dbo.Wip w
        JOIN OrderedAgg oa ON oa.PLU_Int = TRY_CONVERT(int, w.plu)
        WHERE w.status IN ({status_sql})
          AND TRY_CONVERT(int, w.plu) IS NOT NULL
        GROUP BY TRY_CONVERT(int, w.plu)
    )
    SELECT
        (SELECT COUNT(*) FROM OrdersForDay) AS SO_Count,
        (SELECT COUNT(*) FROM OrderedAgg) AS OrderedPLU_Count,
        (SELECT COUNT(*) FROM ShippedAgg) AS ShippedPLU_Count,
        (SELECT COUNT(*) FROM InvAgg) AS InvPLU_Count;
    """

    params = (schedule_date.isoformat(), schedule_date.isoformat())
    counts = pd.read_sql(sql_counts, conn, params=params).iloc[0].to_dict()

    sql_samples_so = f"""
    SELECT TOP 10 so.TxnID, so.{so_date_field} AS DateValue
    FROM WPL.dbo.GP_SalesOrder so
    WHERE so.{so_date_field} >= CAST(? AS datetime2(0))
      AND so.{so_date_field} <  DATEADD(day, 1, CAST(? AS datetime2(0)))
    ORDER BY so.TxnID;
    """
    so_sample = pd.read_sql(sql_samples_so, conn, params=params)

    sql_samples_detail = f"""
    WITH OrdersForDay AS (
        SELECT so.TxnID
        FROM WPL.dbo.GP_SalesOrder so
        WHERE so.{so_date_field} >= CAST(? AS datetime2(0))
          AND so.{so_date_field} <  DATEADD(day, 1, CAST(? AS datetime2(0)))
    )
    SELECT TOP 10
        d.TxnIDKey,
        d.ItemRef_FullName,
        d.Quantity
    FROM WPL.dbo.GP_SalesOrderLineDetail d
    JOIN OrdersForDay o ON o.TxnID = d.TxnIDKey
    ORDER BY d.TxnIDKey;
    """
    detail_sample = pd.read_sql(sql_samples_detail, conn, params=params)

    sql_samples_shipped = """
    SELECT TOP 10
        s.OrderNum,
        s.ProductNumber,
        s.QtyShipped,
        s.DateTimeStamp
    FROM WPL.dbo.Shipped s
    ORDER BY s.DateTimeStamp DESC;
    """
    shipped_sample = pd.read_sql(sql_samples_shipped, conn)

    return {
        "counts": counts,
        "so_sample": so_sample,
        "detail_sample": detail_sample,
        "shipped_sample": shipped_sample,
    }


def fetch_shortsheets(conn, schedule_date: date, so_date_field: str, statuses: list[str], only_remaining: bool) -> pd.DataFrame:
    allowed = {"TxnDate", "ShipDate", "DueDate", "TimeCreated"}
    if so_date_field not in allowed:
        raise ValueError(f"Invalid SO date field '{so_date_field}'")

    if not statuses:
        statuses = ["Available"]

    status_sql = _build_in_list_sql(statuses)

    where_remaining = "WHERE (oa.QtyOrdered - COALESCE(sa.QtyShipped,0)) > 0" if only_remaining else ""

    sql = f"""
    WITH OrdersForDay AS (
        SELECT so.TxnID
        FROM WPL.dbo.GP_SalesOrder so
        WHERE so.{so_date_field} >= CAST(? AS datetime2(0))
          AND so.{so_date_field} <  DATEADD(day, 1, CAST(? AS datetime2(0)))
    ),
    OrderedAgg AS (
        SELECT
            TRY_CONVERT(int, d.ItemRef_FullName) AS PLU_Int,
            SUM(COALESCE(d.Quantity,0)) AS QtyOrdered
        FROM WPL.dbo.GP_SalesOrderLineDetail d
        JOIN OrdersForDay o ON o.TxnID = d.TxnIDKey
        WHERE TRY_CONVERT(int, d.ItemRef_FullName) IS NOT NULL
        GROUP BY TRY_CONVERT(int, d.ItemRef_FullName)
    ),
    ShippedAgg AS (
        SELECT
            TRY_CONVERT(int, s.ProductNumber) AS PLU_Int,
            SUM(COALESCE(s.QtyShipped,0)) AS QtyShipped
        FROM WPL.dbo.Shipped s
        JOIN OrdersForDay o ON o.TxnID = s.OrderNum
        WHERE TRY_CONVERT(int, s.ProductNumber) IS NOT NULL
        GROUP BY TRY_CONVERT(int, s.ProductNumber)
    ),
    InvAgg AS (
        SELECT
            TRY_CONVERT(int, w.plu) AS PLU_Int,
            COUNT(*) AS AvailableCases
        FROM WPL.dbo.Wip w
        JOIN OrderedAgg oa ON oa.PLU_Int = TRY_CONVERT(int, w.plu)
        WHERE w.status IN ({status_sql})
          AND TRY_CONVERT(int, w.plu) IS NOT NULL
        GROUP BY TRY_CONVERT(int, w.plu)
    )
    SELECT
        oa.PLU_Int,
        oa.QtyOrdered,
        COALESCE(sa.QtyShipped,0) AS QtyShipped,
        COALESCE(ia.AvailableCases,0) AS AvailableCases,
        (oa.QtyOrdered - COALESCE(sa.QtyShipped,0)) AS RemainingCases
    FROM OrderedAgg oa
    LEFT JOIN ShippedAgg sa ON sa.PLU_Int = oa.PLU_Int
    LEFT JOIN InvAgg ia ON ia.PLU_Int = oa.PLU_Int
    {where_remaining}
    ORDER BY RemainingCases DESC, oa.PLU_Int;
    """

    params = (schedule_date.isoformat(), schedule_date.isoformat())
    df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return df

    df["PLU"] = df["PLU_Int"].astype(int).astype(str).str.zfill(5)
    df = df.drop(columns=["PLU_Int"])
    return df


# -----------------------------
# Product master merge
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


def infer_product_cols(df: pd.DataFrame) -> dict:
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


def merge_master(short_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    if master_df.empty:
        return short_df

    mcols = infer_product_cols(master_df)
    if not mcols["plu"]:
        return short_df

    m = master_df.copy()
    m[mcols["plu"]] = m[mcols["plu"]].astype(str).str.strip().str.zfill(5)

    keep = [mcols["plu"]]
    if mcols["desc"]:
        keep.append(mcols["desc"])
    m2 = m[keep].drop_duplicates(subset=[mcols["plu"]])

    out = short_df.copy()
    out["PLU"] = out["PLU"].astype(str).str.strip().str.zfill(5)

    merged = out.merge(m2, how="left", left_on="PLU", right_on=mcols["plu"])

    if mcols["plu"] in merged.columns:
        merged = merged.drop(columns=[mcols["plu"]])

    if mcols["desc"] and mcols["desc"] in merged.columns:
        merged = merged.rename(columns={mcols["desc"]: "ProductDescription"})

    desired = ["PLU", "ProductDescription", "QtyOrdered", "QtyShipped", "AvailableCases", "RemainingCases"]
    cols = [c for c in desired if c in merged.columns] + [c for c in merged.columns if c not in desired]
    return merged[cols]


def export_to_excel(df: pd.DataFrame, out_path: str) -> None:
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Shortsheet", index=False)


# -----------------------------
# GUI App
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("980x700")

        self.cfg = load_config()

        # Connection vars
        self.var_server = tk.StringVar(value=self.cfg.server)
        self.var_database = tk.StringVar(value=self.cfg.database)
        self.var_driver = tk.StringVar(value=self.cfg.driver)
        self.var_auth = tk.StringVar(value=self.cfg.auth_mode)
        self.var_user = tk.StringVar(value=self.cfg.username)
        self.var_pass = tk.StringVar(value=self.cfg.password)

        # Product vars
        self.var_product_path = tk.StringVar(value=self.cfg.product_excel_path)
        self.var_product_sheet = tk.StringVar(value=self.cfg.product_sheet_name)

        # Run vars
        self.var_schedule_date = tk.StringVar(value=date.today().isoformat())
        self.var_so_date_field = tk.StringVar(value="ShipDate")
        self.var_only_remaining = tk.BooleanVar(value=True)

        # WIP statuses
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
        ttk.Button(frm_conn, text="Test Connection", command=self.on_test_connection).grid(row=row, column=0, sticky="w", **pad)
        ttk.Button(frm_conn, text="Save Settings", command=self.on_save_settings).grid(row=row, column=1, sticky="w", **pad)

        frm_prod = ttk.LabelFrame(self, text="Product Master Excel (optional for description)")
        frm_prod.pack(fill="x", **pad)

        row = 0
        ttk.Label(frm_prod, text="Excel File Path:").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_prod, textvariable=self.var_product_path, width=72).grid(row=row, column=1, sticky="w", **pad)
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
        ttk.Label(frm_run, text="WIP statuses included in AvailableCases:").grid(row=row, column=0, sticky="w", **pad)
        ttk.Checkbutton(frm_run, text="Available", variable=self.var_wip_available).grid(row=row, column=1, sticky="w", **pad)
        ttk.Checkbutton(frm_run, text="ScanningSalesOrder", variable=self.var_wip_scanning).grid(row=row, column=2, sticky="w", **pad)
        ttk.Checkbutton(frm_run, text="WaitingToBeInvoiced", variable=self.var_wip_waiting).grid(row=row, column=3, sticky="w", **pad)

        row += 1
        ttk.Label(frm_run, text="Output Folder:").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(frm_run, textvariable=self.var_output_folder, width=45).grid(row=row, column=1, sticky="w", **pad)
        ttk.Button(frm_run, text="Browse...", command=self.on_browse_output).grid(row=row, column=2, sticky="w", **pad)

        ttk.Button(frm_run, text="Build Shortsheet", command=self.on_build).grid(row=row, column=3, sticky="w", **pad)
        ttk.Button(frm_run, text="Run Diagnostics", command=self.on_diagnostics).grid(row=row, column=4, sticky="w", **pad)

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

    def _parse_date(self) -> date:
        return datetime.strptime(self.var_schedule_date.get().strip(), "%Y-%m-%d").date()

    def _connect(self, cfg: AppConfig):
        cs = build_connection_string(cfg)
        return pyodbc.connect(cs, timeout=20)

    def _get_status_list(self) -> list[str]:
        statuses = []
        if self.var_wip_available.get():
            statuses.append("Available")
        if self.var_wip_scanning.get():
            statuses.append("ScanningSalesOrder")
        if self.var_wip_waiting.get():
            statuses.append("WaitingToBeInvoiced")
        return statuses

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

    def on_diagnostics(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed.")
            return

        cfg = self._collect_config()
        statuses = self._get_status_list()
        so_field = self.var_so_date_field.get().strip()
        only_remaining = bool(self.var_only_remaining.get())

        try:
            sched = self._parse_date()
        except ValueError:
            messagebox.showerror("Invalid Date", "Enter date as YYYY-MM-DD.")
            return

        try:
            self._log("Connecting to SQL Server for diagnostics...")
            with self._connect(cfg) as conn:
                diag = run_diagnostics(conn, sched, so_field, statuses)

            c = diag["counts"]
            self._log(f"Diagnostics for {sched.isoformat()} using SO.{so_field}")
            self._log(f"  SO_Count: {c.get('SO_Count')}")
            self._log(f"  OrderedPLU_Count: {c.get('OrderedPLU_Count')}")
            self._log(f"  ShippedPLU_Count: {c.get('ShippedPLU_Count')}")
            self._log(f"  InvPLU_Count: {c.get('InvPLU_Count')}")
            self._log(f"  OnlyRemaining setting: {only_remaining}")

            self._log("Sample SO TxnIDs:")
            for _, r in diag["so_sample"].iterrows():
                self._log(f"  {r['TxnID']} | {r['DateValue']}")

            self._log("Sample SO Detail lines:")
            for _, r in diag["detail_sample"].iterrows():
                self._log(f"  TxnIDKey={r['TxnIDKey']}  PLU={r['ItemRef_FullName']}  Qty={r['Quantity']}")

            self._log("Latest Shipped rows:")
            for _, r in diag["shipped_sample"].iterrows():
                self._log(f"  OrderNum={r['OrderNum']}  PLU={r['ProductNumber']}  Shipped={r['QtyShipped']}  DT={r['DateTimeStamp']}")

            messagebox.showinfo("Diagnostics", "Diagnostics written to log.")
        except Exception as e:
            self._log("ERROR diagnostics:")
            self._log(str(e))
            self._log(traceback.format_exc())
            messagebox.showerror("Diagnostics Error", str(e))

    def on_build(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed.")
            return

        cfg = self._collect_config()
        statuses = self._get_status_list()
        so_field = self.var_so_date_field.get().strip()
        only_remaining = bool(self.var_only_remaining.get())

        try:
            sched = self._parse_date()
        except ValueError:
            messagebox.showerror("Invalid Date", "Enter date as YYYY-MM-DD.")
            return

        # Load product master (optional)
        master_df = pd.DataFrame()
        try:
            master_df = load_product_master(cfg.product_excel_path, cfg.product_sheet_name)
            if not master_df.empty:
                self._log(f"Loaded product master rows: {len(master_df):,}")
        except Exception as e:
            self._log(f"WARNING: product master load failed: {e}")
            master_df = pd.DataFrame()

        try:
            self._log("Connecting to SQL Server...")
            with self._connect(cfg) as conn:
                self._log(f"Running shortsheet for {sched.isoformat()} using SO.{so_field} ...")
                df = fetch_shortsheets(conn, sched, so_field, statuses, only_remaining)
            self._log(f"Rows returned: {len(df):,}")
        except Exception as e:
            self._log("ERROR running shortsheet query:")
            self._log(str(e))
            self._log(traceback.format_exc())
            messagebox.showerror("SQL Error", str(e))
            return

        if df.empty:
            self._log("No results. Try changing SO Date Field (TxnDate vs ShipDate) and run Diagnostics.")
            messagebox.showinfo("No Results", "No results. Try changing SO Date Field and run Diagnostics.")
            return

        # Merge description (optional)
        try:
            if not master_df.empty:
                df = merge_master(df, master_df)
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
