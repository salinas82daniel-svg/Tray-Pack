import json
import os
import traceback
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import pandas as pd

try:
    import pyodbc
except ImportError:
    pyodbc = None


APP_NAME = "ShortsheetBuilder"
CONFIG_FILE = "config.json"

# Your mapping
MACHINE_OSSID = "O"
MACHINE_REPAK = "R"


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
            merged = {**AppConfig().__dict__, **(data or {})}
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
    return s.replace("'", "''")


def _build_in_list_sql(values: list[str]) -> str:
    # Safe for constants you control (checkbox values)
    return ", ".join("'" + _escape_sql_literal(v) + "'" for v in values)


def sql_int_expr(col_name: str) -> str:
    # Works on older SQL Server: ISNUMERIC + CAST
    return f"(CASE WHEN ISNUMERIC({col_name}) = 1 THEN CAST({col_name} AS INT) ELSE NULL END)"


def sql_num_expr(col_name: str) -> str:
    return f"(CASE WHEN ISNUMERIC({col_name}) = 1 THEN CAST({col_name} AS FLOAT) ELSE 0 END)"


def safe_float(x) -> float:
    try:
        if pd.isna(x):
            return 0.0
        return float(x)
    except Exception:
        return 0.0


# -----------------------------
# Product master
# -----------------------------
def load_product_master(path: str, sheet_name: str = "") -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()

    if sheet_name.strip():
        df = pd.read_excel(path, sheet_name=sheet_name.strip())
    else:
        df = pd.read_excel(path)

    df.columns = [str(c).strip() for c in df.columns]

    # Expected columns (flexible): PLU, DESC, Trays, TPM, Machine, Type, Frozen
    if "PLU" in df.columns:
        df["PLU"] = df["PLU"].astype(str).str.strip().str.zfill(5)
    else:
        raise ValueError("Product master must contain a column named 'PLU'.")

    if "DESC" in df.columns:
        df["DESC"] = df["DESC"].astype(str).fillna("")
    else:
        df["DESC"] = ""

    if "Trays" in df.columns:
        df["Trays"] = pd.to_numeric(df["Trays"], errors="coerce").fillna(0).astype(float)
    else:
        df["Trays"] = 0.0

    if "TPM" in df.columns:
        df["TPM"] = pd.to_numeric(df["TPM"], errors="coerce").fillna(0).astype(float)
    else:
        df["TPM"] = 0.0

    if "Machine" in df.columns:
        df["Machine"] = df["Machine"].astype(str).str.strip()
    else:
        df["Machine"] = ""

    if "Type" in df.columns:
        df["Type"] = df["Type"].astype(str).str.strip()
    else:
        df["Type"] = ""

    if "Frozen" in df.columns:
        df["Frozen"] = df["Frozen"].astype(str).str.strip().str.upper()
    else:
        df["Frozen"] = ""

    return df


def product_machines(master_df: pd.DataFrame) -> list[str]:
    if master_df.empty or "Machine" not in master_df.columns:
        return ["All"]
    machines = sorted({str(x).strip() for x in master_df["Machine"].dropna().unique() if str(x).strip()})
    return ["All"] + machines


# -----------------------------
# Shortsheet SQL (TxnDate)
# Remaining = Ordered - Shipped - AvailableCases
# -----------------------------
def fetch_shortsheets(conn, schedule_date: date, wip_statuses: list[str], only_remaining: bool) -> pd.DataFrame:
    if not wip_statuses:
        wip_statuses = ["Available"]

    status_sql = _build_in_list_sql(wip_statuses)

    d_plu = sql_int_expr("d.ItemRef_FullName")
    s_plu = sql_int_expr("s.ProductNumber")
    w_plu = sql_int_expr("w.plu")

    d_qty = sql_num_expr("d.Quantity")
    s_qty = sql_num_expr("s.QtyShipped")

    remaining_expr = "(oa.QtyOrdered - ISNULL(sa.QtyShipped,0) - ISNULL(ia.AvailableCases,0))"
    where_remaining = f"WHERE {remaining_expr} > 0" if only_remaining else ""

    sql = f"""
    WITH OrdersForDay AS (
        SELECT so.TxnID
        FROM WPL.dbo.GP_SalesOrder so
        WHERE so.TxnDate >= CAST(? AS DATETIME)
          AND so.TxnDate <  DATEADD(day, 1, CAST(? AS DATETIME))
    ),
    OrderedAgg AS (
        SELECT
            {d_plu} AS PLU_Int,
            SUM({d_qty}) AS QtyOrdered
        FROM WPL.dbo.GP_SalesOrderLineDetail d
        JOIN OrdersForDay o ON o.TxnID = d.TxnIDKey
        WHERE {d_plu} IS NOT NULL
        GROUP BY {d_plu}
    ),
    ShippedAgg AS (
        SELECT
            {s_plu} AS PLU_Int,
            SUM({s_qty}) AS QtyShipped
        FROM WPL.dbo.Shipped s
        JOIN OrdersForDay o ON o.TxnID = s.OrderNum
        WHERE {s_plu} IS NOT NULL
        GROUP BY {s_plu}
    ),
    InvAgg AS (
        SELECT
            {w_plu} AS PLU_Int,
            COUNT(*) AS AvailableCases
        FROM WPL.dbo.Wip w
        JOIN OrderedAgg oa ON oa.PLU_Int = {w_plu}
        WHERE w.status IN ({status_sql})
          AND {w_plu} IS NOT NULL
        GROUP BY {w_plu}
    )
    SELECT
        oa.PLU_Int,
        oa.QtyOrdered,
        ISNULL(sa.QtyShipped,0) AS QtyShipped,
        ISNULL(ia.AvailableCases,0) AS AvailableCases,
        {remaining_expr} AS RemainingCases
    FROM OrderedAgg oa
    LEFT JOIN ShippedAgg sa ON sa.PLU_Int = oa.PLU_Int
    LEFT JOIN InvAgg ia ON ia.PLU_Int = oa.PLU_Int
    {where_remaining}
    ORDER BY RemainingCases DESC, oa.PLU_Int;
    """

    df = pd.read_sql(sql, conn, params=(schedule_date.isoformat(), schedule_date.isoformat()))
    if df.empty:
        return df

    df["ProductNumber"] = df["PLU_Int"].astype(int)
    df["PLU"] = df["PLU_Int"].astype(int).astype(str).str.zfill(5)
    df = df.drop(columns=["PLU_Int"])
    df["RemainingCases"] = df["RemainingCases"].apply(lambda x: x if safe_float(x) > 0 else 0)

    return df[["ProductNumber", "PLU", "QtyOrdered", "QtyShipped", "AvailableCases", "RemainingCases"]]


# -----------------------------
# Production SQL (PackDate) includes Shipped
# -----------------------------
def fetch_production_by_packdate(conn, packdate_value, statuses: list[str]) -> pd.DataFrame:
    if not statuses:
        statuses = ["Available", "ScanningSalesOrder", "WaitingToBeInvoiced", "Shipped"]

    status_sql = _build_in_list_sql(statuses)
    w_plu = sql_int_expr("w.plu")
    w_wt = sql_num_expr("w.casewt")

    sql = f"""
    SELECT
        {w_plu} AS PLU_Int,
        COUNT(*) AS CasesProduced,
        SUM({w_wt}) AS LbsProduced
    FROM WPL.dbo.Wip w
    WHERE w.packdate = ?
      AND w.status IN ({status_sql})
      AND {w_plu} IS NOT NULL
    GROUP BY {w_plu}
    ORDER BY {w_plu};
    """

    df = pd.read_sql(sql, conn, params=(packdate_value,))
    if df.empty:
        return df

    df["ProductNumber"] = df["PLU_Int"].astype(int)
    df["PLU"] = df["PLU_Int"].astype(int).astype(str).str.zfill(5)
    df = df.drop(columns=["PLU_Int"])
    return df


# -----------------------------
# Export helpers
# -----------------------------
def export_to_excel(df: pd.DataFrame, out_path: str, sheet_name: str) -> None:
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


def export_multi_to_excel(tables: dict[str, pd.DataFrame], out_path: str) -> None:
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for name, df in tables.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)


# -----------------------------
# Time helpers
# -----------------------------
def parse_hhmm(s: str) -> time:
    return datetime.strptime((s or "").strip(), "%H:%M").time()


def minutes_between(start: datetime, end: datetime) -> float:
    return max(0.0, (end - start).total_seconds() / 60.0)


# -----------------------------
# UI helper: Treeview
# -----------------------------
class TableView(ttk.Frame):
    def __init__(self, parent, columns: list[str], height: int = 12):
        super().__init__(parent)
        self.columns = columns
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=height)
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.vsb.set, xscrollcommand=self.hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        self.vsb.grid(row=0, column=1, sticky="ns")
        self.hsb.grid(row=1, column=0, sticky="ew")

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        for c in columns:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=120, anchor="w")

    def set_dataframe(self, df: pd.DataFrame):
        for item in self.tree.get_children():
            self.tree.delete(item)

        if df is None or df.empty:
            return

        cols = list(df.columns)
        if cols != self.columns:
            self.columns = cols
            self.tree.configure(columns=cols)
            for c in cols:
                self.tree.heading(c, text=c)
                self.tree.column(c, width=120, anchor="w")

        for _, row in df.iterrows():
            values = [row.get(c, "") for c in self.columns]
            self.tree.insert("", "end", values=values)


# -----------------------------
# Dashboard widgets
# -----------------------------
class KpiBlock(tk.Frame):
    def __init__(self, parent, title: str, value_color="#00ff00"):
        super().__init__(parent, bg="#000000")
        self.title = tk.Label(self, text=title, bg="#000000", fg="#00ff00", font=("Segoe UI", 18, "bold"))
        self.value = tk.Label(self, text="—", bg="#000000", fg=value_color, font=("Segoe UI", 22, "bold"))
        self.title.pack(anchor="center", pady=(2, 0))
        self.value.pack(anchor="center", pady=(0, 8))

    def set_value(self, text: str):
        self.value.configure(text=text)


class RatioBlock(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg="#000000")
        tk.Label(self, text=title, bg="#000000", fg="#00ff00", font=("Segoe UI", 16, "bold")).pack(pady=(0, 6))

        grid = tk.Frame(self, bg="#000000")
        grid.pack()

        tk.Label(grid, text="OSSID", bg="#000000", fg="#00ff00", font=("Segoe UI", 12, "bold")).grid(row=0, column=0, padx=18)
        tk.Label(grid, text="REPAK", bg="#000000", fg="#00ff00", font=("Segoe UI", 12, "bold")).grid(row=0, column=1, padx=18)

        self.ossid = tk.Label(grid, text="—", bg="#000000", fg="#00ff00", font=("Segoe UI", 16, "bold"))
        self.repak = tk.Label(grid, text="—", bg="#000000", fg="#00ff00", font=("Segoe UI", 16, "bold"))
        self.ossid.grid(row=1, column=0, padx=18)
        self.repak.grid(row=1, column=1, padx=18)

    def set_values(self, ossid_val: str, repak_val: str):
        self.ossid.configure(text=ossid_val)
        self.repak.configure(text=repak_val)


# -----------------------------
# Main App
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1500x920")

        self.cfg = load_config()

        self.master_df: pd.DataFrame | None = None
        self.last_shortsheets_df: pd.DataFrame | None = None
        self.last_production_df: pd.DataFrame | None = None

        # Connection vars
        self.var_server = tk.StringVar(value=self.cfg.server)
        self.var_database = tk.StringVar(value=self.cfg.database)
        self.var_driver = tk.StringVar(value=self.cfg.driver)
        self.var_auth = tk.StringVar(value=self.cfg.auth_mode)
        self.var_user = tk.StringVar(value=self.cfg.username)
        self.var_pass = tk.StringVar(value=self.cfg.password)

        # Product master vars
        self.var_product_path = tk.StringVar(value=self.cfg.product_excel_path)
        self.var_product_sheet = tk.StringVar(value=self.cfg.product_sheet_name)

        # Output
        self.var_output_folder = tk.StringVar(value=self.cfg.output_folder or os.getcwd())

        # Options
        self.var_exclude_missing = tk.BooleanVar(value=True)
        self.var_exclude_frozen = tk.BooleanVar(value=False)

        # Shortsheet inputs
        self.var_txndate = tk.StringVar(value=date.today().isoformat())
        self.var_only_remaining = tk.BooleanVar(value=True)
        self.var_wip_available = tk.BooleanVar(value=True)
        self.var_wip_scanning = tk.BooleanVar(value=True)
        self.var_wip_waiting = tk.BooleanVar(value=True)

        # Production inputs
        self.var_packdate = tk.StringVar(value="")
        self.var_machine = tk.StringVar(value="All")
        self.var_start_time = tk.StringVar(value="08:30")
        self.var_end_time = tk.StringVar(value="17:00")

        self.production_statuses = ["Available", "ScanningSalesOrder", "WaitingToBeInvoiced", "Shipped"]

        self._build_ui()
        self._refresh_auth_state()
        self._log("Ready.")
        self._load_master(initial=True)

    # ---------------- UI ----------------
    def _build_ui(self):
        pad = {"padx": 10, "pady": 6}

        frm_top = ttk.LabelFrame(self, text="Connection + Files")
        frm_top.pack(fill="x", **pad)

        r = 0
        ttk.Label(frm_top, text="Server:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm_top, textvariable=self.var_server, width=32).grid(row=r, column=1, sticky="w", **pad)

        ttk.Label(frm_top, text="Database:").grid(row=r, column=2, sticky="w", **pad)
        ttk.Entry(frm_top, textvariable=self.var_database, width=12).grid(row=r, column=3, sticky="w", **pad)

        ttk.Label(frm_top, text="Driver:").grid(row=r, column=4, sticky="w", **pad)
        drivers = list_odbc_drivers()
        self.cmb_driver = ttk.Combobox(frm_top, textvariable=self.var_driver, values=drivers, width=28)
        self.cmb_driver.grid(row=r, column=5, sticky="w", **pad)
        if drivers and not self.var_driver.get():
            self.var_driver.set(drivers[0])

        r += 1
        ttk.Label(frm_top, text="Auth:").grid(row=r, column=0, sticky="w", **pad)
        self.cmb_auth = ttk.Combobox(frm_top, textvariable=self.var_auth, values=["windows", "sql"], width=10, state="readonly")
        self.cmb_auth.grid(row=r, column=1, sticky="w", **pad)
        self.cmb_auth.bind("<<ComboboxSelected>>", lambda e: self._refresh_auth_state())

        ttk.Label(frm_top, text="User:").grid(row=r, column=2, sticky="w", **pad)
        self.ent_user = ttk.Entry(frm_top, textvariable=self.var_user, width=20)
        self.ent_user.grid(row=r, column=3, sticky="w", **pad)

        ttk.Label(frm_top, text="Pass:").grid(row=r, column=4, sticky="w", **pad)
        self.ent_pass = ttk.Entry(frm_top, textvariable=self.var_pass, width=18, show="*")
        self.ent_pass.grid(row=r, column=5, sticky="w", **pad)

        r += 1
        ttk.Label(frm_top, text="Product Excel:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm_top, textvariable=self.var_product_path, width=60).grid(row=r, column=1, columnspan=3, sticky="w", **pad)
        ttk.Button(frm_top, text="Browse", command=self.on_browse_product).grid(row=r, column=4, sticky="w", **pad)
        ttk.Button(frm_top, text="Load Master", command=self._load_master).grid(row=r, column=5, sticky="w", **pad)

        r += 1
        ttk.Label(frm_top, text="Output Folder:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm_top, textvariable=self.var_output_folder, width=60).grid(row=r, column=1, columnspan=3, sticky="w", **pad)
        ttk.Button(frm_top, text="Browse", command=self.on_browse_output).grid(row=r, column=4, sticky="w", **pad)
        ttk.Button(frm_top, text="Test Connection", command=self.on_test_connection).grid(row=r, column=5, sticky="w", **pad)

        r += 1
        ttk.Checkbutton(frm_top, text="Exclude PLUs missing from Product Info.xlsx", variable=self.var_exclude_missing).grid(
            row=r, column=0, columnspan=3, sticky="w", **pad
        )
        ttk.Checkbutton(frm_top, text="Exclude Frozen Items (Frozen = Y)", variable=self.var_exclude_frozen).grid(
            row=r, column=3, columnspan=2, sticky="w", **pad
        )
        ttk.Button(frm_top, text="Save Settings", command=self.on_save_settings).grid(row=r, column=5, sticky="w", **pad)

        # Tabs
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, **pad)

        self.tab_dash = tk.Frame(self.nb, bg="#000000")
        self.tab_short = ttk.Frame(self.nb)
        self.tab_prod = ttk.Frame(self.nb)

        self.nb.add(self.tab_dash, text="Summary Dashboard")
        self.nb.add(self.tab_short, text="Shortsheet (TxnDate)")
        self.nb.add(self.tab_prod, text="Production (PackDate)")

        self._build_tab_dashboard()
        self._build_tab_shortsheets()
        self._build_tab_production()

        # Log
        frm_log = ttk.LabelFrame(self, text="Log")
        frm_log.pack(fill="both", expand=False, **pad)
        self.txt_log = tk.Text(frm_log, height=10, wrap="word")
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=10)

    def _build_tab_dashboard(self):
        self.lbl_dash_time = tk.Label(self.tab_dash, text="—", bg="#000000", fg="#00ff00", font=("Segoe UI", 10, "bold"))
        self.lbl_dash_time.place(relx=0.98, rely=0.02, anchor="ne")

        row1 = tk.Frame(self.tab_dash, bg="#000000")
        row1.pack(fill="x", padx=30, pady=(25, 10))

        self.kpi_traypack = KpiBlock(row1, "TRAY PACK")
        self.kpi_traysmin = KpiBlock(row1, "TRAYS/MIN")
        self.kpi_trays_to_complete = KpiBlock(row1, "TRAYS TO COMPLETE", value_color="#ff2b2b")

        self.kpi_traypack.pack(side="left", padx=40)
        self.kpi_traysmin.pack(side="left", padx=40)
        self.kpi_trays_to_complete.pack(side="right", padx=40)

        row2 = tk.Frame(self.tab_dash, bg="#000000")
        row2.pack(fill="x", padx=30, pady=(0, 10))

        self.tray_completed = RatioBlock(row2, "RUN RATIO (TRAYS) - COMPLETED")
        self.tray_remaining = RatioBlock(row2, "RUN RATIO (TRAYS) - REMAINING")
        self.tray_completed.pack(side="left", padx=60)
        self.tray_remaining.pack(side="left", padx=60)

        row3 = tk.Frame(self.tab_dash, bg="#000000")
        row3.pack(fill="x", padx=30, pady=(10, 10))

        self.kpi_tpcases = KpiBlock(row3, "TP CASES")
        self.kpi_casesmin = KpiBlock(row3, "CASES/MIN")
        self.kpi_cases_to_complete = KpiBlock(row3, "CASES TO COMPLETE", value_color="#ff2b2b")

        self.kpi_tpcases.pack(side="left", padx=40)
        self.kpi_casesmin.pack(side="left", padx=40)
        self.kpi_cases_to_complete.pack(side="right", padx=40)

        row4 = tk.Frame(self.tab_dash, bg="#000000")
        row4.pack(fill="x", padx=30, pady=(0, 10))

        self.case_completed = RatioBlock(row4, "CASE RATIO - COMPLETED")
        self.case_remaining = RatioBlock(row4, "CASE RATIO - REMAINING")
        self.case_completed.pack(side="left", padx=60)
        self.case_remaining.pack(side="left", padx=60)

        sep = tk.Frame(self.tab_dash, bg="#00ff00", height=2)
        sep.pack(fill="x", padx=10, pady=(10, 10))

        lbl = tk.Label(self.tab_dash, text="ESTIMATED TIME OF COMPLETION", bg="#000000", fg="#00ff00",
                       font=("Segoe UI", 22, "bold"))
        lbl.pack(pady=(0, 8))

        self.lbl_estimates = tk.Label(self.tab_dash, text="Run Shortsheet + Production then Refresh Dashboard.",
                                      bg="#000000", fg="#00ff00", font=("Segoe UI", 12, "bold"), justify="left")
        self.lbl_estimates.pack(anchor="w", padx=60, pady=(0, 10))

        btn = tk.Button(self.tab_dash, text="REFRESH DASHBOARD", command=self.refresh_dashboard,
                        bg="#101010", fg="#00ff00", activebackground="#202020", activeforeground="#00ff00",
                        relief="solid", bd=1, font=("Segoe UI", 12, "bold"))
        btn.pack(anchor="w", padx=60, pady=(0, 10))

        self.refresh_dashboard()

    def _build_tab_shortsheets(self):
        pad = {"padx": 10, "pady": 6}

        frm = ttk.LabelFrame(self.tab_short, text="Inputs")
        frm.pack(fill="x", **pad)

        r = 0
        ttk.Label(frm, text="TxnDate (YYYY-MM-DD):").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_txndate, width=16).grid(row=r, column=1, sticky="w", **pad)
        ttk.Checkbutton(frm, text="Only Remaining (>0)", variable=self.var_only_remaining).grid(row=r, column=2, sticky="w", **pad)

        r += 1
        ttk.Label(frm, text="WIP statuses counted as AvailableCases:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Checkbutton(frm, text="Available", variable=self.var_wip_available).grid(row=r, column=1, sticky="w", **pad)
        ttk.Checkbutton(frm, text="ScanningSalesOrder", variable=self.var_wip_scanning).grid(row=r, column=2, sticky="w", **pad)
        ttk.Checkbutton(frm, text="WaitingToBeInvoiced", variable=self.var_wip_waiting).grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Button(frm, text="Run Shortsheet", command=self.on_run_shortsheets).grid(row=r, column=0, sticky="w", **pad)
        ttk.Button(frm, text="Export Shortsheet Excel", command=self.on_export_shortsheets).grid(row=r, column=1, sticky="w", **pad)

        self.short_table = TableView(self.tab_short, columns=[
            "ProductNumber", "PLU", "ProductDescription", "QtyOrdered", "QtyShipped", "AvailableCases", "RemainingCases"
        ], height=14)
        self.short_table.pack(fill="both", expand=True, padx=10, pady=10)

    def _build_tab_production(self):
        pad = {"padx": 10, "pady": 6}

        frm = ttk.LabelFrame(self.tab_prod, text="Inputs")
        frm.pack(fill="x", **pad)

        r = 0
        ttk.Label(frm, text="PackDate (4-digit code):").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_packdate, width=10).grid(row=r, column=1, sticky="w", **pad)

        ttk.Label(frm, text="Machine:").grid(row=r, column=2, sticky="w", **pad)
        self.cmb_machine = ttk.Combobox(frm, textvariable=self.var_machine, values=["All"], width=10, state="readonly")
        self.cmb_machine.grid(row=r, column=3, sticky="w", **pad)

        ttk.Label(frm, text="Start (HH:MM):").grid(row=r, column=4, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_start_time, width=8).grid(row=r, column=5, sticky="w", **pad)

        ttk.Label(frm, text="End (HH:MM):").grid(row=r, column=6, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_end_time, width=8).grid(row=r, column=7, sticky="w", **pad)

        r += 1
        ttk.Button(frm, text="Refresh Production", command=self.on_refresh_production).grid(row=r, column=0, sticky="w", **pad)
        ttk.Button(frm, text="Export Production Excel", command=self.on_export_production).grid(row=r, column=1, sticky="w", **pad)

        self.prod_summary = tk.Text(self.tab_prod, height=8, wrap="word")
        self.prod_summary.pack(fill="x", padx=10, pady=10)

        self.prod_table = TableView(self.tab_prod, columns=[
            "Machine", "Type", "ProductNumber", "PLU", "DESC", "TraysPerCase", "StdTPM",
            "CasesProduced", "LbsProduced", "TraysProduced"
        ], height=10)
        ttk.Label(self.tab_prod, text="Production by PLU").pack(anchor="w", padx=10)
        self.prod_table.pack(fill="both", expand=True, padx=10, pady=6)

        self.primal_table = TableView(self.tab_prod, columns=[
            "Machine", "Type", "CasesProduced", "LbsProduced", "TraysProduced"
        ], height=8)
        ttk.Label(self.tab_prod, text="Production by Primal (Type)").pack(anchor="w", padx=10)
        self.primal_table.pack(fill="both", expand=True, padx=10, pady=6)

    # -------------- common helpers --------------
    def _log(self, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.txt_log.insert("end", f"[{ts}] {msg}\n")
        self.txt_log.see("end")
        self.update_idletasks()

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

    def _parse_txndate(self) -> date:
        return datetime.strptime(self.var_txndate.get().strip(), "%Y-%m-%d").date()

    def _get_short_wip_statuses(self) -> list[str]:
        statuses = []
        if self.var_wip_available.get():
            statuses.append("Available")
        if self.var_wip_scanning.get():
            statuses.append("ScanningSalesOrder")
        if self.var_wip_waiting.get():
            statuses.append("WaitingToBeInvoiced")
        return statuses or ["Available"]

    def _load_master(self, initial: bool = False):
        try:
            path = self.var_product_path.get().strip()
            sheet = self.var_product_sheet.get().strip()
            if not path or not os.path.exists(path):
                if not initial:
                    messagebox.showwarning("Product Master", "Product Excel not found. Browse to your Product Info.xlsx.")
                self.master_df = pd.DataFrame()
                if hasattr(self, "cmb_machine"):
                    self.cmb_machine.configure(values=["All"])
                    self.var_machine.set("All")
                return

            self._log(f"Loading product master: {path}")
            self.master_df = load_product_master(path, sheet)
            self._log(f"Loaded product master rows: {len(self.master_df):,}")

            machines = product_machines(self.master_df)
            if hasattr(self, "cmb_machine"):
                self.cmb_machine.configure(values=machines)
                if self.var_machine.get() not in machines:
                    self.var_machine.set("All")
        except Exception as e:
            self._log(f"ERROR loading product master: {e}")
            self._log(traceback.format_exc())
            if not initial:
                messagebox.showerror("Product Master Error", str(e))
            self.master_df = pd.DataFrame()

    # -------------- buttons --------------
    def on_browse_product(self):
        path = filedialog.askopenfilename(
            title="Select Product Info Excel",
            filetypes=[("Excel Files", "*.xlsx *.xls"), ("All Files", "*.*")]
        )
        if path:
            self.var_product_path.set(path)

    def on_browse_output(self):
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.var_output_folder.set(folder)

    def on_save_settings(self):
        self.cfg = self._collect_config()
        save_config(self.cfg)
        self._log("Settings saved to config.json")

    def on_test_connection(self):
        cfg = self._collect_config()
        ok, msg = test_connection(cfg)
        self._log(msg)
        if ok:
            messagebox.showinfo("Connection", msg)
        else:
            messagebox.showerror("Connection", msg)

    # ---------------- Dashboard math ----------------
    def refresh_dashboard(self):
        now = datetime.now()
        self.lbl_dash_time.configure(text=now.strftime("%m/%d/%Y %H:%M"))

        trays_completed_total = 0.0
        cases_completed_total = 0.0
        trays_completed_ossid = 0.0
        trays_completed_repak = 0.0
        cases_completed_ossid = 0.0
        cases_completed_repak = 0.0

        trays_remaining_total = 0.0
        cases_remaining_total = 0.0
        trays_remaining_ossid = 0.0
        trays_remaining_repak = 0.0
        cases_remaining_ossid = 0.0
        cases_remaining_repak = 0.0

        # Completed from production
        if self.last_production_df is not None and not self.last_production_df.empty:
            p = self.last_production_df.copy()
            trays_completed_total = float(p["TraysProduced"].sum())
            cases_completed_total = float(p["CasesProduced"].sum())

            po = p[p["Machine"].astype(str).str.strip() == MACHINE_OSSID]
            pr = p[p["Machine"].astype(str).str.strip() == MACHINE_REPAK]
            trays_completed_ossid = float(po["TraysProduced"].sum()) if not po.empty else 0.0
            trays_completed_repak = float(pr["TraysProduced"].sum()) if not pr.empty else 0.0
            cases_completed_ossid = float(po["CasesProduced"].sum()) if not po.empty else 0.0
            cases_completed_repak = float(pr["CasesProduced"].sum()) if not pr.empty else 0.0

        # Remaining from shortsheet
        if self.last_shortsheets_df is not None and not self.last_shortsheets_df.empty and self.master_df is not None and not self.master_df.empty:
            ss = self.last_shortsheets_df.copy()
            m = self.master_df.copy()
            ss["PLU"] = ss["PLU"].astype(str).str.zfill(5)
            m["PLU"] = m["PLU"].astype(str).str.zfill(5)

            ss = ss.merge(m[["PLU", "Trays", "Machine"]], on="PLU", how="left")
            ss["Trays"] = pd.to_numeric(ss["Trays"], errors="coerce").fillna(0).astype(float)
            ss["RemainingCases"] = pd.to_numeric(ss["RemainingCases"], errors="coerce").fillna(0).astype(float)
            ss["Machine"] = ss["Machine"].fillna("Unknown").astype(str).str.strip()
            ss["TraysRemaining"] = ss["RemainingCases"] * ss["Trays"]

            trays_remaining_total = float(ss["TraysRemaining"].sum())
            cases_remaining_total = float(ss["RemainingCases"].sum())

            so = ss[ss["Machine"] == MACHINE_OSSID]
            sr = ss[ss["Machine"] == MACHINE_REPAK]
            trays_remaining_ossid = float(so["TraysRemaining"].sum()) if not so.empty else 0.0
            trays_remaining_repak = float(sr["TraysRemaining"].sum()) if not sr.empty else 0.0
            cases_remaining_ossid = float(so["RemainingCases"].sum()) if not so.empty else 0.0
            cases_remaining_repak = float(sr["RemainingCases"].sum()) if not sr.empty else 0.0

        # Minutes ran based on entered start time
        trays_per_min = 0.0
        cases_per_min = 0.0
        try:
            st = parse_hhmm(self.var_start_time.get())
            start_dt = datetime.combine(now.date(), st)
            mins = minutes_between(start_dt, now)
            if mins > 0:
                trays_per_min = trays_completed_total / mins
                cases_per_min = cases_completed_total / mins
        except Exception:
            pass

        self.kpi_traypack.set_value(f"{trays_completed_total:,.0f}" if trays_completed_total > 0 else "—")
        self.kpi_traysmin.set_value(f"{trays_per_min:,.2f}" if trays_per_min > 0 else "—")
        self.kpi_tpcases.set_value(f"{cases_completed_total:,.0f}" if cases_completed_total > 0 else "—")
        self.kpi_casesmin.set_value(f"{cases_per_min:,.2f}" if cases_per_min > 0 else "—")

        self.kpi_trays_to_complete.set_value(f"{trays_remaining_total:,.0f}" if trays_remaining_total > 0 else "—")
        self.kpi_cases_to_complete.set_value(f"{cases_remaining_total:,.0f}" if cases_remaining_total > 0 else "—")

        self.tray_completed.set_values(f"{trays_completed_ossid:,.0f}", f"{trays_completed_repak:,.0f}")
        self.tray_remaining.set_values(f"{trays_remaining_ossid:,.0f}", f"{trays_remaining_repak:,.0f}")
        self.case_completed.set_values(f"{cases_completed_ossid:,.0f}", f"{cases_completed_repak:,.0f}")
        self.case_remaining.set_values(f"{cases_remaining_ossid:,.0f}", f"{cases_remaining_repak:,.0f}")

        lines = []
        if trays_remaining_total > 0 and trays_per_min > 0:
            est_min = trays_remaining_total / trays_per_min
            finish = now + timedelta(minutes=est_min)
            lines.append(f"Based on ACTUAL -> {trays_per_min:,.2f} trays/min | Est: {est_min/60:,.2f} hrs | Finish: {finish.strftime('%H:%M')}")
        else:
            lines.append("Based on ACTUAL -> (Run Production + Shortsheet)")

        # Standards estimate (weighted)
        if self.last_production_df is not None and not self.last_production_df.empty and trays_remaining_total > 0:
            p = self.last_production_df.copy()
            total_trays_prod = float(p["TraysProduced"].sum())
            std_weighted = 0.0
            if total_trays_prod > 0:
                std_weighted = float((p["StdTPM"] * p["TraysProduced"]).sum() / total_trays_prod)

            if std_weighted > 0:
                std_min = trays_remaining_total / std_weighted
                std_finish = now + timedelta(minutes=std_min)
                lines.append(f"Based on STANDARDS -> {std_weighted:,.2f} trays/min | Est: {std_min/60:,.2f} hrs | Finish: {std_finish.strftime('%H:%M')}")
            else:
                lines.append("Based on STANDARDS -> StdTPM missing/0 in Product Info")
        else:
            lines.append("Based on STANDARDS -> (Run Production + Shortsheet)")

        self.lbl_estimates.configure(text="\n".join(lines))

    # ---------------- Shortsheet actions ----------------
    def on_run_shortsheets(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed.")
            return

        self._load_master(initial=True)

        try:
            txnd = self._parse_txndate()
        except ValueError:
            messagebox.showerror("TxnDate", "Enter TxnDate as YYYY-MM-DD.")
            return

        statuses = self._get_short_wip_statuses()
        only_remaining = bool(self.var_only_remaining.get())
        exclude_missing = bool(self.var_exclude_missing.get())
        exclude_frozen = bool(self.var_exclude_frozen.get())

        try:
            cfg = self._collect_config()
            self._log("Connecting to SQL Server (shortsheet)...")
            with pyodbc.connect(build_connection_string(cfg), timeout=25) as conn:
                df = fetch_shortsheets(conn, txnd, statuses, only_remaining)

            if df.empty:
                self._log("Shortsheet returned 0 rows.")
                self.last_shortsheets_df = df
                self.short_table.set_dataframe(pd.DataFrame(columns=self.short_table.columns))
                self.refresh_dashboard()
                messagebox.showinfo("Shortsheet", "No remaining balances found.")
                return

            df2 = df.copy()

            # Merge product master for description
            if self.master_df is not None and not self.master_df.empty:
                m = self.master_df.copy()
                df2["PLU"] = df2["PLU"].astype(str).str.zfill(5)
                df2 = df2.merge(m[["PLU", "DESC", "Trays", "TPM", "Type", "Machine", "Frozen"]], on="PLU", how="left")
                df2 = df2.rename(columns={"DESC": "ProductDescription", "Trays": "TraysPerCase", "TPM": "StdTPM"})
            else:
                df2["ProductDescription"] = ""
                df2["TraysPerCase"] = None
                df2["StdTPM"] = 0
                df2["Frozen"] = ""

            # Exclude missing
            if exclude_missing and self.master_df is not None and not self.master_df.empty:
                df2["TraysPerCase"] = pd.to_numeric(df2["TraysPerCase"], errors="coerce")
                before = len(df2)
                df2 = df2[df2["TraysPerCase"].notna()].copy()
                after = len(df2)
                self._log(f"Exclude missing Product Info: removed {before - after} rows")

            # Exclude frozen
            if exclude_frozen:
                before = len(df2)
                df2["Frozen"] = df2["Frozen"].astype(str).str.strip().str.upper()
                df2 = df2[df2["Frozen"] != "Y"].copy()
                after = len(df2)
                self._log(f"Exclude Frozen=Y: removed {before - after} rows")

            show_cols = ["ProductNumber", "PLU", "ProductDescription", "QtyOrdered", "QtyShipped", "AvailableCases", "RemainingCases"]
            for c in show_cols:
                if c not in df2.columns:
                    df2[c] = ""
            df2 = df2[show_cols]

            self.last_shortsheets_df = df2
            self.short_table.set_dataframe(df2)
            self._log(f"Shortsheet rows: {len(df2):,}")
            self.refresh_dashboard()
        except Exception as e:
            self._log(f"ERROR shortsheet: {e}")
            self._log(traceback.format_exc())
            messagebox.showerror("Shortsheet Error", str(e))

    def on_export_shortsheets(self):
        if self.last_shortsheets_df is None or self.last_shortsheets_df.empty:
            messagebox.showinfo("Export", "Run Shortsheet first.")
            return

        out_folder = self.var_output_folder.get().strip() or os.getcwd()
        os.makedirs(out_folder, exist_ok=True)

        try:
            txnd = self._parse_txndate()
        except ValueError:
            txnd = date.today()

        out_path = os.path.join(out_folder, f"Shortsheet_TxnDate_{txnd.isoformat()}.xlsx")
        try:
            export_to_excel(self.last_shortsheets_df, out_path, "Shortsheet")
            self._log(f"Exported shortsheet: {out_path}")
            messagebox.showinfo("Export", f"Saved:\n{out_path}")
        except Exception as e:
            self._log(f"ERROR exporting shortsheet: {e}")
            messagebox.showerror("Export Error", str(e))

    # ---------------- Production actions ----------------
    def on_refresh_production(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed.")
            return

        self._load_master(initial=True)
        if self.master_df is None or self.master_df.empty:
            messagebox.showerror("Product Master", "Load Product Info.xlsx first.")
            return

        pack = (self.var_packdate.get() or "").strip()
        if not pack:
            messagebox.showerror("PackDate", "Enter the 4-digit packdate code (e.g., 1307).")
            return

        pack_param = int(pack) if pack.isdigit() else pack
        machine_filter = (self.var_machine.get() or "All").strip()
        exclude_missing = bool(self.var_exclude_missing.get())
        exclude_frozen = bool(self.var_exclude_frozen.get())

        try:
            st = parse_hhmm(self.var_start_time.get())
            et = parse_hhmm(self.var_end_time.get())
        except Exception:
            messagebox.showerror("Time", "Enter Start/End time as HH:MM (24-hour), e.g., 08:30")
            return

        now = datetime.now()
        start_dt = datetime.combine(now.date(), st)
        end_dt = datetime.combine(now.date(), et)
        if end_dt <= start_dt:
            end_dt = end_dt + timedelta(days=1)

        minutes_ran = minutes_between(start_dt, now)
        minutes_left_clock = max(0.0, minutes_between(now, end_dt))

        try:
            cfg = self._collect_config()
            self._log("Connecting to SQL Server (production)...")
            with pyodbc.connect(build_connection_string(cfg), timeout=25) as conn:
                prod_df = fetch_production_by_packdate(conn, pack_param, self.production_statuses)

            if prod_df.empty:
                self._log("Production query returned 0 rows.")
                self.last_production_df = pd.DataFrame()
                self.prod_table.set_dataframe(pd.DataFrame(columns=self.prod_table.columns))
                self.primal_table.set_dataframe(pd.DataFrame(columns=self.primal_table.columns))
                self.refresh_dashboard()
                messagebox.showinfo("Production", "No production found for that packdate/status set.")
                return

            m = self.master_df.copy()
            prod_df["PLU"] = prod_df["PLU"].astype(str).str.zfill(5)
            merged = prod_df.merge(m[["PLU", "DESC", "Trays", "TPM", "Type", "Machine", "Frozen"]], on="PLU", how="left")

            merged = merged.rename(columns={"Trays": "TraysPerCase", "TPM": "StdTPM"})
            merged["TraysPerCase"] = pd.to_numeric(merged["TraysPerCase"], errors="coerce")
            merged["StdTPM"] = pd.to_numeric(merged["StdTPM"], errors="coerce").fillna(0).astype(float)
            merged["CasesProduced"] = pd.to_numeric(merged["CasesProduced"], errors="coerce").fillna(0).astype(float)
            merged["LbsProduced"] = pd.to_numeric(merged["LbsProduced"], errors="coerce").fillna(0).astype(float)

            if exclude_missing:
                before = len(merged)
                merged = merged[merged["TraysPerCase"].notna()].copy()
                after = len(merged)
                self._log(f"Exclude missing Product Info: removed {before - after} rows")

            if exclude_frozen:
                before = len(merged)
                merged["Frozen"] = merged["Frozen"].astype(str).str.strip().str.upper()
                merged = merged[merged["Frozen"] != "Y"].copy()
                after = len(merged)
                self._log(f"Exclude Frozen=Y: removed {before - after} rows")

            merged["Machine"] = merged["Machine"].fillna("Unknown").astype(str).str.strip()
            merged["Type"] = merged["Type"].fillna("Unknown")
            merged["DESC"] = merged["DESC"].fillna("")
            merged["TraysPerCase"] = merged["TraysPerCase"].fillna(0).astype(float)

            if machine_filter != "All":
                merged = merged[merged["Machine"].astype(str).str.strip() == machine_filter].copy()

            merged["TraysProduced"] = merged["CasesProduced"] * merged["TraysPerCase"]

            # Save for dashboard
            self.last_production_df = merged.copy()

            total_cases = float(merged["CasesProduced"].sum())
            total_lbs = float(merged["LbsProduced"].sum())
            total_trays = float(merged["TraysProduced"].sum())
            actual_tpm = (total_trays / minutes_ran) if minutes_ran > 0 else 0.0

            trays_left = 0.0
            if self.last_shortsheets_df is not None and not self.last_shortsheets_df.empty and self.master_df is not None and not self.master_df.empty:
                ss = self.last_shortsheets_df.copy()
                ss["PLU"] = ss["PLU"].astype(str).str.zfill(5)
                ss2 = ss.merge(self.master_df[["PLU", "Trays"]], on="PLU", how="left")
                ss2["Trays"] = pd.to_numeric(ss2["Trays"], errors="coerce").fillna(0).astype(float)
                ss2["RemainingCases"] = pd.to_numeric(ss2["RemainingCases"], errors="coerce").fillna(0).astype(float)
                trays_left = float((ss2["RemainingCases"] * ss2["Trays"]).sum())

            est_minutes_left = (trays_left / actual_tpm) if actual_tpm > 0 else 0.0
            projected_finish = (now + timedelta(minutes=est_minutes_left)) if actual_tpm > 0 else None

            std_weighted_tpm = 0.0
            if total_trays > 0:
                std_weighted_tpm = float((merged["StdTPM"] * merged["TraysProduced"]).sum() / total_trays)
            std_minutes_left = (trays_left / std_weighted_tpm) if std_weighted_tpm > 0 else 0.0
            std_finish = (now + timedelta(minutes=std_minutes_left)) if std_weighted_tpm > 0 else None

            merged_display = merged[[
                "Machine", "Type", "ProductNumber", "PLU", "DESC", "TraysPerCase", "StdTPM",
                "CasesProduced", "LbsProduced", "TraysProduced"
            ]].copy().sort_values(by=["Machine", "Type", "TraysProduced"], ascending=[True, True, False])

            primal = merged.groupby(["Machine", "Type"], as_index=False).agg(
                CasesProduced=("CasesProduced", "sum"),
                LbsProduced=("LbsProduced", "sum"),
                TraysProduced=("TraysProduced", "sum"),
            ).sort_values(by=["Machine", "TraysProduced"], ascending=[True, False])

            self.prod_summary.delete("1.0", "end")
            lines = []
            lines.append(f"PackDate: {pack} | Machine filter: {machine_filter}")
            lines.append(f"Statuses counted: {', '.join(self.production_statuses)}")
            lines.append(f"Exclude Frozen=Y: {exclude_frozen} | Exclude Missing Master: {exclude_missing}")
            lines.append("")
            lines.append(f"TOTAL Produced -> Cases: {total_cases:,.0f} | Lbs: {total_lbs:,.1f} | Trays: {total_trays:,.0f}")
            lines.append(f"Run -> Start: {start_dt.strftime('%H:%M')} | Now: {now.strftime('%H:%M')} | Minutes ran: {minutes_ran:,.1f}")
            lines.append(f"Clock -> End: {end_dt.strftime('%H:%M')} | Minutes left (clock): {minutes_left_clock:,.1f}")
            lines.append("")
            lines.append(f"Actual TPM so far: {actual_tpm:,.2f}")
            lines.append(f"Trays left to pack (from Shortsheet): {trays_left:,.0f}" if trays_left > 0 else "Trays left to pack: (Run Shortsheet to populate)")
            if projected_finish is not None:
                lines.append(f"Estimated minutes left (actual): {est_minutes_left:,.1f} | Projected finish: {projected_finish.strftime('%H:%M')}")
            else:
                lines.append("Estimated minutes left (actual): N/A (Actual TPM is 0)")
            lines.append("")
            lines.append(f"Standard TPM (weighted): {std_weighted_tpm:,.2f}")
            if std_finish is not None:
                lines.append(f"Estimated minutes left (standard): {std_minutes_left:,.1f} | Standard finish: {std_finish.strftime('%H:%M')}")
            else:
                lines.append("Estimated minutes left (standard): N/A (StdTPM is 0 or missing)")

            self.prod_summary.insert("end", "\n".join(lines))

            self.prod_table.set_dataframe(merged_display)
            self.primal_table.set_dataframe(primal)

            self._log(f"Production rows (PLU): {len(merged_display):,}")
            self.refresh_dashboard()
        except Exception as e:
            self._log(f"ERROR production: {e}")
            self._log(traceback.format_exc())
            messagebox.showerror("Production Error", str(e))

    def on_export_production(self):
        if self.last_production_df is None or self.last_production_df.empty:
            messagebox.showinfo("Export", "Refresh Production first.")
            return

        out_folder = self.var_output_folder.get().strip() or os.getcwd()
        os.makedirs(out_folder, exist_ok=True)

        pack = (self.var_packdate.get() or "").strip() or "PackDate"
        machine_filter = (self.var_machine.get() or "All").strip()

        out_path = os.path.join(out_folder, f"Production_PackDate_{pack}_{machine_filter}.xlsx")
        try:
            merged = self.last_production_df.copy()
            by_plu = merged[[
                "Machine", "Type", "ProductNumber", "PLU", "DESC", "TraysPerCase", "StdTPM",
                "CasesProduced", "LbsProduced", "TraysProduced"
            ]].copy().sort_values(by=["Machine", "Type", "TraysProduced"], ascending=[True, True, False])

            by_type = merged.groupby(["Machine", "Type"], as_index=False).agg(
                CasesProduced=("CasesProduced", "sum"),
                LbsProduced=("LbsProduced", "sum"),
                TraysProduced=("TraysProduced", "sum"),
            ).sort_values(by=["Machine", "TraysProduced"], ascending=[True, False])

            export_multi_to_excel({"ProductionByPLU": by_plu, "ProductionByType": by_type}, out_path)
            self._log(f"Exported production: {out_path}")
            messagebox.showinfo("Export", f"Saved:\n{out_path}")
        except Exception as e:
            self._log(f"ERROR exporting production: {e}")
            messagebox.showerror("Export Error", str(e))

    # ---------------- misc ----------------
    def on_browse_product(self):
        path = filedialog.askopenfilename(
            title="Select Product Info Excel",
            filetypes=[("Excel Files", "*.xlsx *.xls"), ("All Files", "*.*")]
        )
        if path:
            self.var_product_path.set(path)

    def on_browse_output(self):
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.var_output_folder.set(folder)

    def on_save_settings(self):
        self.cfg = self._collect_config()
        save_config(self.cfg)
        self._log("Settings saved to config.json")

    def on_test_connection(self):
        cfg = self._collect_config()
        ok, msg = test_connection(cfg)
        self._log(msg)
        if ok:
            messagebox.showinfo("Connection", msg)
        else:
            messagebox.showerror("Connection", msg)


def main():
    App().mainloop()


if __name__ == "__main__":
    main()
