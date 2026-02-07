import json
import os
import traceback
from dataclasses import dataclass, field
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

# Machine mapping (your rule)
MACHINE_OSSID = "O"
MACHINE_REPAK = "R"

# Fed sort order (custom)
FED_ORDER = ["Organic", "ABF", "Heirloom", "Legacy", "NGMO", "Frozen", "Duck", "Halal"]


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
    exclude_missing_master: bool = True
    exclude_frozen_y: bool = False

    # Planned shorts persisted by TxnDate RANGE key -> ["00123","04567",...]
    # Range key format: YYYY-MM-DD__YYYY-MM-DD
    planned_shorts: dict = field(default_factory=dict)


def load_config() -> AppConfig:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            merged = {**AppConfig().__dict__, **data}
            if not isinstance(merged.get("planned_shorts"), dict):
                merged["planned_shorts"] = {}
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
    return ", ".join("'" + _escape_sql_literal(v) + "'" for v in values)


def sql_int_expr(col_name: str) -> str:
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


def norm_plu(s: str) -> str:
    s = str(s or "").strip()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        return digits.zfill(5)
    return s.zfill(5)


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

    if "PLU" not in df.columns:
        raise ValueError("Product master must contain a column named 'PLU'.")

    df["PLU"] = df["PLU"].astype(str).str.strip().str.zfill(5)

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

    df["Machine"] = df["Machine"].astype(str).str.strip() if "Machine" in df.columns else ""
    df["Type"] = df["Type"].astype(str).str.strip() if "Type" in df.columns else ""
    df["Frozen"] = df["Frozen"].astype(str).str.strip().str.upper() if "Frozen" in df.columns else ""
    df["Fed"] = df["Fed"].astype(str).str.strip() if "Fed" in df.columns else ""
    df["Fed"] = df["Fed"].fillna("").astype(str).str.strip()

    return df


def product_machines(master_df: pd.DataFrame) -> list[str]:
    if master_df.empty or "Machine" not in master_df.columns:
        return ["All"]
    machines = sorted({str(x).strip() for x in master_df["Machine"].dropna().unique() if str(x).strip()})
    return ["All"] + machines


# -----------------------------
# Shortsheet SQL (TxnDate RANGE)
# Remaining = Ordered - Shipped - AvailableCases
# -----------------------------
def fetch_shortsheets_range(conn, from_date: date, to_date: date, wip_statuses: list[str], only_remaining: bool) -> pd.DataFrame:
    if not wip_statuses:
        wip_statuses = ["Available"]

    if to_date < from_date:
        from_date, to_date = to_date, from_date

    status_sql = _build_in_list_sql(wip_statuses)

    d_plu = sql_int_expr("d.ItemRef_FullName")
    s_plu = sql_int_expr("s.ProductNumber")
    w_plu = sql_int_expr("w.plu")

    d_qty = sql_num_expr("d.Quantity")
    s_qty = sql_num_expr("s.QtyShipped")

    remaining_expr = "(oa.QtyOrdered - ISNULL(sa.QtyShipped,0) - ISNULL(ia.AvailableCases,0))"
    where_remaining = f"WHERE {remaining_expr} > 0" if only_remaining else ""

    sql = f"""
    WITH OrdersForRange AS (
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
        JOIN OrdersForRange o ON o.TxnID = d.TxnIDKey
        WHERE {d_plu} IS NOT NULL
        GROUP BY {d_plu}
    ),
    ShippedAgg AS (
        SELECT
            {s_plu} AS PLU_Int,
            SUM({s_qty}) AS QtyShipped
        FROM WPL.dbo.Shipped s
        JOIN OrdersForRange o ON o.TxnID = s.OrderNum
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

    df = pd.read_sql(sql, conn, params=(from_date.isoformat(), to_date.isoformat()))
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
# Time helpers
# -----------------------------
def parse_hhmm(s: str) -> time:
    return datetime.strptime((s or "").strip(), "%H:%M").time()


def minutes_between(start: datetime, end: datetime) -> float:
    return max(0.0, (end - start).total_seconds() / 60.0)


def end_datetime_for_today(start_dt: datetime, end_t: time) -> datetime:
    end_dt = datetime.combine(start_dt.date(), end_t)
    if end_dt < start_dt:
        end_dt = end_dt + timedelta(days=1)
    return end_dt


def fmt_duration_minutes(mins: float) -> str:
    mins = max(0.0, float(mins))
    h = int(mins // 60)
    m = int(round(mins - (h * 60)))
    if h <= 0:
        return f"{m} min"
    return f"{h} hr {m} min"


# -----------------------------
# UI helper: Treeview
# -----------------------------
class TableView(ttk.Frame):
    def __init__(self, parent, columns: list[str], height: int = 12):
        super().__init__(parent)
        self.columns = columns
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=height, selectmode="browse")
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
            self.tree.column(c, width=140, anchor="w")

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
                self.tree.column(c, width=140, anchor="w")

        for _, row in df.iterrows():
            values = [row.get(c, "") for c in self.columns]
            self.tree.insert("", "end", values=values)

    def get_selected_values(self) -> dict:
        sel = self.tree.selection()
        if not sel:
            return {}
        item = self.tree.item(sel[0])
        vals = item.get("values", [])
        if not vals:
            return {}
        return {self.columns[i]: vals[i] for i in range(min(len(self.columns), len(vals)))}

    def select_first_match(self, predicate):
        for iid in self.tree.get_children():
            vals = self.tree.item(iid).get("values", [])
            row = {self.columns[i]: vals[i] for i in range(min(len(self.columns), len(vals)))}
            if predicate(row):
                self.tree.selection_set(iid)
                self.tree.see(iid)
                return True
        return False


# -----------------------------
# Dashboard widgets
# -----------------------------
class KpiBlock(tk.Frame):
    def __init__(self, parent, title: str, value_color="#00ff00"):
        super().__init__(parent, bg="#000000")
        self.title = tk.Label(self, text=title, bg="#000000", fg="#00ff00", font=("Segoe UI", 18, "bold"))
        self.value = tk.Label(self, text="—", bg="#000000", fg=value_color, font=("Segoe UI", 20, "bold"))
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
# Settings popup
# -----------------------------
class SettingsWindow(tk.Toplevel):
    def __init__(self, parent, cfg, on_apply_callback):
        super().__init__(parent)
        self.title("Settings")
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()

        self.on_apply_callback = on_apply_callback

        self.var_server = tk.StringVar(value=cfg.server)
        self.var_database = tk.StringVar(value=cfg.database)
        self.var_driver = tk.StringVar(value=cfg.driver)
        self.var_auth = tk.StringVar(value=cfg.auth_mode)
        self.var_user = tk.StringVar(value=cfg.username)
        self.var_pass = tk.StringVar(value=cfg.password)

        self.var_product_path = tk.StringVar(value=cfg.product_excel_path)
        self.var_output_folder = tk.StringVar(value=cfg.output_folder or os.getcwd())

        self.var_exclude_missing = tk.BooleanVar(value=cfg.exclude_missing_master)
        self.var_exclude_frozen = tk.BooleanVar(value=cfg.exclude_frozen_y)

        pad = {"padx": 10, "pady": 6}

        frm = ttk.Frame(self)
        frm.pack(fill="both", expand=True, padx=10, pady=10)

        r = 0
        ttk.Label(frm, text="Server:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_server, width=30).grid(row=r, column=1, sticky="w", **pad)

        ttk.Label(frm, text="Database:").grid(row=r, column=2, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_database, width=10).grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Label(frm, text="Driver:").grid(row=r, column=0, sticky="w", **pad)
        drivers = list_odbc_drivers()
        self.cmb_driver = ttk.Combobox(frm, textvariable=self.var_driver, values=drivers, width=28)
        self.cmb_driver.grid(row=r, column=1, sticky="w", **pad)
        if drivers and not self.var_driver.get():
            self.var_driver.set(drivers[0])

        ttk.Label(frm, text="Auth:").grid(row=r, column=2, sticky="w", **pad)
        self.cmb_auth = ttk.Combobox(frm, textvariable=self.var_auth, values=["windows", "sql"], width=10, state="readonly")
        self.cmb_auth.grid(row=r, column=3, sticky="w", **pad)
        self.cmb_auth.bind("<<ComboboxSelected>>", lambda e: self._refresh_auth_state())

        r += 1
        ttk.Label(frm, text="User:").grid(row=r, column=0, sticky="w", **pad)
        self.ent_user = ttk.Entry(frm, textvariable=self.var_user, width=20)
        self.ent_user.grid(row=r, column=1, sticky="w", **pad)

        ttk.Label(frm, text="Pass:").grid(row=r, column=2, sticky="w", **pad)
        self.ent_pass = ttk.Entry(frm, textvariable=self.var_pass, width=18, show="*")
        self.ent_pass.grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Label(frm, text="Product Excel:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_product_path, width=55).grid(row=r, column=1, columnspan=2, sticky="w", **pad)
        ttk.Button(frm, text="Browse", command=self._browse_product).grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Label(frm, text="Output Folder:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_output_folder, width=55).grid(row=r, column=1, columnspan=2, sticky="w", **pad)
        ttk.Button(frm, text="Browse", command=self._browse_output).grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Checkbutton(frm, text="Exclude PLUs missing from Product Info.xlsx", variable=self.var_exclude_missing).grid(
            row=r, column=0, columnspan=2, sticky="w", **pad
        )
        ttk.Checkbutton(frm, text="Exclude Frozen Items (Frozen = Y)", variable=self.var_exclude_frozen).grid(
            row=r, column=2, columnspan=2, sticky="w", **pad
        )

        r += 1
        btns = ttk.Frame(frm)
        btns.grid(row=r, column=0, columnspan=4, sticky="e", padx=10, pady=(10, 0))

        ttk.Button(btns, text="Test Connection", command=self._test_connection).pack(side="left", padx=8)
        ttk.Button(btns, text="Save", command=self._save).pack(side="left", padx=8)
        ttk.Button(btns, text="Close", command=self.destroy).pack(side="left", padx=8)

        self._refresh_auth_state()

    def _refresh_auth_state(self):
        is_sql = self.var_auth.get().strip().lower() == "sql"
        state = "normal" if is_sql else "disabled"
        self.ent_user.configure(state=state)
        self.ent_pass.configure(state=state)

    def _browse_product(self):
        path = filedialog.askopenfilename(
            title="Select Product Info Excel",
            filetypes=[("Excel Files", "*.xlsx *.xls"), ("All Files", "*.*")]
        )
        if path:
            self.var_product_path.set(path)

    def _browse_output(self):
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.var_output_folder.set(folder)

    def _test_connection(self):
        cfg = self._build_cfg()
        ok, msg = test_connection(cfg)
        if ok:
            messagebox.showinfo("Connection", msg, parent=self)
        else:
            messagebox.showerror("Connection", msg, parent=self)

    def _build_cfg(self) -> AppConfig:
        return AppConfig(
            server=self.var_server.get().strip(),
            database=self.var_database.get().strip() or "WPL",
            driver=self.var_driver.get().strip(),
            auth_mode=self.var_auth.get().strip().lower(),
            username=self.var_user.get().strip(),
            password=self.var_pass.get(),
            product_excel_path=self.var_product_path.get().strip(),
            output_folder=self.var_output_folder.get().strip(),
            exclude_missing_master=bool(self.var_exclude_missing.get()),
            exclude_frozen_y=bool(self.var_exclude_frozen.get()),
            planned_shorts={},  # replaced on apply
        )

    def _save(self):
        cfg = self._build_cfg()
        cfg.planned_shorts = getattr(self.master, "cfg", AppConfig()).planned_shorts or {}
        try:
            save_config(cfg)
            self.on_apply_callback(cfg)
            messagebox.showinfo("Settings", "Saved.", parent=self)
        except Exception as e:
            messagebox.showerror("Settings", str(e), parent=self)


# -----------------------------
# Category Dashboard (popup)
# -----------------------------
class CategoryDashboard(tk.Toplevel):
    def __init__(self, parent, title: str, compute_callback):
        super().__init__(parent)
        self.title(title)
        self.geometry("1500x860")
        self.configure(bg="#000000")
        self.compute_callback = compute_callback

        self.lbl_time = tk.Label(self, text="—", bg="#000000", fg="#00ff00", font=("Segoe UI", 10, "bold"))
        self.lbl_time.place(relx=0.98, rely=0.02, anchor="ne")

        row1 = tk.Frame(self, bg="#000000")
        row1.pack(fill="x", padx=30, pady=(25, 10))

        self.kpi_traypack = KpiBlock(row1, "TRAY PACK")
        self.kpi_trays_to_complete = KpiBlock(row1, "TRAYS TO COMPLETE", value_color="#ff2b2b")
        self.kpi_planned = KpiBlock(row1, "PLANNED SHORTS", value_color="#ff2b2b")

        self.kpi_traypack.pack(side="left", padx=30)
        tk.Frame(row1, bg="#000000", width=50).pack(side="left")
        self.kpi_planned.pack(side="right", padx=30)
        self.kpi_trays_to_complete.pack(side="right", padx=30)

        row2 = tk.Frame(self, bg="#000000")
        row2.pack(fill="x", padx=30, pady=(0, 10))
        self.tray_completed = RatioBlock(row2, "TRAYS COMPLETED")
        self.tray_remaining = RatioBlock(row2, "TRAYS REMAINING")
        self.tray_completed.pack(side="left", padx=60)
        self.tray_remaining.pack(side="left", padx=60)

        row3 = tk.Frame(self, bg="#000000")
        row3.pack(fill="x", padx=30, pady=(10, 10))

        self.kpi_tpcases = KpiBlock(row3, "TP CASES")
        self.kpi_cases_to_complete = KpiBlock(row3, "CASES TO COMPLETE", value_color="#ff2b2b")
        self.kpi_tpcases.pack(side="left", padx=40)
        self.kpi_cases_to_complete.pack(side="right", padx=40)

        row4 = tk.Frame(self, bg="#000000")
        row4.pack(fill="x", padx=30, pady=(0, 10))
        self.case_completed = RatioBlock(row4, "CASES COMPLETED")
        self.case_remaining = RatioBlock(row4, "CASES REMAINING")
        self.case_completed.pack(side="left", padx=60)
        self.case_remaining.pack(side="left", padx=60)

        sep = tk.Frame(self, bg="#00ff00", height=2)
        sep.pack(fill="x", padx=10, pady=(10, 10))

        lbl = tk.Label(self, text="HOURS NEEDED (NOT A CLOCK TIME)", bg="#000000", fg="#00ff00",
                       font=("Segoe UI", 22, "bold"))
        lbl.pack(pady=(0, 8))

        self.lbl_estimates = tk.Label(self, text="—", bg="#000000", fg="#00ff00",
                                      font=("Segoe UI", 12, "bold"), justify="left")
        self.lbl_estimates.pack(anchor="w", padx=60, pady=(0, 6))

        ttk.Button(self, text="Refresh", command=self.refresh).pack(pady=8)
        self.refresh()

    def refresh(self):
        now = datetime.now()
        self.lbl_time.configure(text=now.strftime("%m/%d/%Y %H:%M"))

        data = self.compute_callback()
        if not data:
            return

        self.kpi_traypack.set_value(data["tray_pack"])
        self.kpi_trays_to_complete.set_value(data["trays_to_complete"])
        self.kpi_planned.set_value(data["planned"])

        self.tray_completed.set_values(data["trays_done_o"], data["trays_done_r"])
        self.tray_remaining.set_values(data["trays_rem_o"], data["trays_rem_r"])
        self.case_completed.set_values(data["cases_done_o"], data["cases_done_r"])
        self.case_remaining.set_values(data["cases_rem_o"], data["cases_rem_r"])

        self.kpi_tpcases.set_value(data["tp_cases"])
        self.kpi_cases_to_complete.set_value(data["cases_to_complete"])

        self.lbl_estimates.configure(text=data["estimates"])


# -----------------------------
# Main App
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1600x940")

        self.cfg = load_config()

        self.master_df: pd.DataFrame | None = None
        self.last_shortsheets_df: pd.DataFrame | None = None
        self.last_shortsheets_detail: pd.DataFrame | None = None
        self.last_production_df: pd.DataFrame | None = None

        today = date.today().isoformat()
        self.var_txn_from = tk.StringVar(value=today)
        self.var_txn_to = tk.StringVar(value=today)
        self.var_only_remaining = tk.BooleanVar(value=True)
        self.var_wip_available = tk.BooleanVar(value=True)
        self.var_wip_scanning = tk.BooleanVar(value=True)
        self.var_wip_waiting = tk.BooleanVar(value=True)

        self.var_apply_planned_shorts = tk.BooleanVar(value=True)
        self.var_planned_search = tk.StringVar(value="")
        self.var_planned_bulk = tk.StringVar(value="")

        self.var_packdate = tk.StringVar(value="")
        self.var_machine = tk.StringVar(value="All")
        self.var_start_time = tk.StringVar(value="08:30")
        self.var_end_time = tk.StringVar(value="17:00")

        self.var_ossid_lines = tk.IntVar(value=6)
        self.var_repak_lines = tk.IntVar(value=1)

        self.production_statuses = ["Available", "ScanningSalesOrder", "WaitingToBeInvoiced", "Shipped"]

        self._build_ui()
        self._log("Ready.")
        self._load_master(initial=True)
        self._refresh_planned_listbox()

    # ------------- planned shorts persistence (range key) -------------
    def _parse_txn_from_to(self) -> tuple[date, date]:
        d1 = datetime.strptime(self.var_txn_from.get().strip(), "%Y-%m-%d").date()
        d2 = datetime.strptime(self.var_txn_to.get().strip(), "%Y-%m-%d").date()
        if d2 < d1:
            d1, d2 = d2, d1
        return d1, d2

    def _range_key(self) -> str:
        try:
            d1, d2 = self._parse_txn_from_to()
            return f"{d1.isoformat()}__{d2.isoformat()}"
        except Exception:
            t = date.today().isoformat()
            return f"{t}__{t}"

    def _get_planned_set_for_range(self, key: str) -> set[str]:
        d = self.cfg.planned_shorts or {}
        items = d.get(key, []) if isinstance(d, dict) else []
        out = set()
        for x in items:
            p = norm_plu(x)
            if p:
                out.add(p)
        return out

    def _set_planned_for_range(self, key: str, planned_set: set[str]):
        if self.cfg.planned_shorts is None or not isinstance(self.cfg.planned_shorts, dict):
            self.cfg.planned_shorts = {}
        self.cfg.planned_shorts[key] = sorted(planned_set)
        save_config(self.cfg)
        self._log(f"Planned shorts saved for {key}: {len(planned_set)} PLU(s).")

    def _refresh_planned_listbox(self):
        key = self._range_key()
        planned = sorted(self._get_planned_set_for_range(key))
        self.lst_planned.delete(0, "end")
        for p in planned:
            self.lst_planned.insert("end", p)

        self._apply_planned_to_current_frames()
        self.refresh_dashboard()

    def _apply_planned_to_current_frames(self):
        key = self._range_key()
        planned = self._get_planned_set_for_range(key)

        if self.last_shortsheets_detail is not None and not self.last_shortsheets_detail.empty:
            d = self.last_shortsheets_detail.copy()
            d["PLU"] = d["PLU"].astype(str).str.zfill(5)
            d["Excluded"] = d["PLU"].isin(planned)
            self.last_shortsheets_detail = d

        if self.last_shortsheets_df is not None and not self.last_shortsheets_df.empty:
            df = self.last_shortsheets_df.copy()
            df["PLU"] = df["PLU"].astype(str).str.zfill(5)
            df["Excluded"] = df["PLU"].isin(planned)
            self.last_shortsheets_df = df
            self.short_table.set_dataframe(df)

    # ------------- UI -------------
    def _build_ui(self):
        topbar = ttk.Frame(self)
        topbar.pack(fill="x", padx=10, pady=(10, 6))

        ttk.Button(topbar, text="⚙ Settings…", command=self.open_settings).pack(side="left")
        ttk.Label(topbar, text="   ").pack(side="left")
        ttk.Button(topbar, text="Refresh Dashboard", command=self.refresh_dashboard).pack(side="left")

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=10)

        self.tab_dash = ttk.Frame(self.nb)
        self.tab_short = ttk.Frame(self.nb)
        self.tab_prod = ttk.Frame(self.nb)

        self.nb.add(self.tab_dash, text="Summary Dashboard")
        self.nb.add(self.tab_short, text="Shortsheet (TxnDate Range)")
        self.nb.add(self.tab_prod, text="Production (PackDate)")

        self._build_tab_dashboard()
        self._build_tab_shortsheets()
        self._build_tab_production()

        frm_log = ttk.LabelFrame(self, text="Log")
        frm_log.pack(fill="both", expand=False, padx=10, pady=(0, 10))
        self.txt_log = tk.Text(frm_log, height=8, wrap="word")
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=10)

    def open_settings(self):
        SettingsWindow(self, self.cfg, self._apply_settings)

    def _apply_settings(self, cfg: AppConfig):
        cfg.planned_shorts = self.cfg.planned_shorts or {}
        self.cfg = cfg
        self._log("Settings applied.")
        self._load_master(initial=True)
        self._refresh_planned_listbox()

    def _log(self, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.txt_log.insert("end", f"[{ts}] {msg}\n")
        self.txt_log.see("end")
        self.update_idletasks()

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
            path = (self.cfg.product_excel_path or "").strip()
            if not path or not os.path.exists(path):
                self.master_df = pd.DataFrame()
                if hasattr(self, "cmb_machine"):
                    self.cmb_machine.configure(values=["All"])
                    self.var_machine.set("All")
                if not initial:
                    messagebox.showwarning("Product Master", "Product Excel not found. Set it in Settings.")
                return

            self._log(f"Loading product master: {path}")
            self.master_df = load_product_master(path, "")
            self._log(f"Loaded product master rows: {len(self.master_df):,}")

            machines = product_machines(self.master_df)
            if hasattr(self, "cmb_machine"):
                self.cmb_machine.configure(values=machines)
                if self.var_machine.get() not in machines:
                    self.var_machine.set("All")
        except Exception as e:
            self._log(f"ERROR loading product master: {e}")
            self._log(traceback.format_exc())
            self.master_df = pd.DataFrame()
            if not initial:
                messagebox.showerror("Product Master Error", str(e))

    # ---------------- Standard time calculation ----------------
    def compute_standard_minutes(self, remain_df: pd.DataFrame) -> dict:
        """
        remain_df must have: Machine, TraysRemaining, StdTPM
        Standard time uses per-line StdTPM, divided by number of lines running for that machine.
        """
        ossid_lines = max(1, int(self.var_ossid_lines.get() or 1))
        repak_lines = max(1, int(self.var_repak_lines.get() or 1))

        if remain_df is None or remain_df.empty:
            return {"ossid": 0.0, "repak": 0.0, "total": 0.0}

        d = remain_df.copy()
        d["Machine"] = d.get("Machine", "").fillna("Unknown").astype(str).str.strip()
        d["TraysRemaining"] = pd.to_numeric(d.get("TraysRemaining", 0), errors="coerce").fillna(0).astype(float)
        d["StdTPM"] = pd.to_numeric(d.get("StdTPM", 0), errors="coerce").fillna(0).astype(float)

        def std_minutes_row(row):
            t = float(row["TraysRemaining"])
            tpm = float(row["StdTPM"])
            if t <= 0 or tpm <= 0:
                return 0.0
            if row["Machine"] == MACHINE_OSSID:
                return (t / tpm) / ossid_lines
            if row["Machine"] == MACHINE_REPAK:
                return (t / tpm) / repak_lines
            return (t / tpm)

        d["StdMinutes"] = d.apply(std_minutes_row, axis=1)

        ossid = float(d.loc[d["Machine"] == MACHINE_OSSID, "StdMinutes"].sum())
        repak = float(d.loc[d["Machine"] == MACHINE_REPAK, "StdMinutes"].sum())
        total = float(d["StdMinutes"].sum())
        return {"ossid": ossid, "repak": repak, "total": total}

    # ---------------- Dashboard ----------------
    def _build_tab_dashboard(self):
        root = tk.Frame(self.tab_dash, bg="#000000")
        root.pack(fill="both", expand=True)

        self.lbl_dash_time = tk.Label(root, text="—", bg="#000000", fg="#00ff00", font=("Segoe UI", 10, "bold"))
        self.lbl_dash_time.place(relx=0.98, rely=0.02, anchor="ne")

        btns = tk.Frame(root, bg="#000000")
        btns.pack(fill="x", padx=10, pady=(10, 0))
        tk.Label(btns, text="Category Dashboards:", bg="#000000", fg="#00ff00",
                 font=("Segoe UI", 10, "bold")).pack(side="left", padx=(10, 8))

        for cat in ["Organic", "ABF", "Heirloom", "Legacy", "NGMO"]:
            ttk.Button(btns, text=cat, command=lambda c=cat: self.open_category_dashboard(c)).pack(side="left", padx=6)

        row1 = tk.Frame(root, bg="#000000")
        row1.pack(fill="x", padx=30, pady=(15, 10))

        self.kpi_traypack = KpiBlock(row1, "TRAY PACK")
        self.kpi_traysmin = KpiBlock(row1, "TRAYS/MIN")  # overall trays/min stays on main page
        self.kpi_trays_to_complete = KpiBlock(row1, "TRAYS TO COMPLETE", value_color="#ff2b2b")
        self.kpi_planned = KpiBlock(row1, "PLANNED SHORTS", value_color="#ff2b2b")

        self.kpi_traypack.pack(side="left", padx=30)
        self.kpi_traysmin.pack(side="left", padx=30)
        tk.Frame(row1, bg="#000000", width=50).pack(side="left")
        self.kpi_planned.pack(side="right", padx=30)
        self.kpi_trays_to_complete.pack(side="right", padx=30)

        row2 = tk.Frame(root, bg="#000000")
        row2.pack(fill="x", padx=30, pady=(0, 10))

        self.tray_completed = RatioBlock(row2, "RUN RATIO (TRAYS) - COMPLETED")
        self.tray_remaining = RatioBlock(row2, "RUN RATIO (TRAYS) - REMAINING")
        self.tray_completed.pack(side="left", padx=60)
        self.tray_remaining.pack(side="left", padx=60)

        row3 = tk.Frame(root, bg="#000000")
        row3.pack(fill="x", padx=30, pady=(10, 10))

        self.kpi_tpcases = KpiBlock(row3, "TP CASES")
        self.kpi_casesmin = KpiBlock(row3, "CASES/MIN")
        self.kpi_cases_to_complete = KpiBlock(row3, "CASES TO COMPLETE", value_color="#ff2b2b")

        self.kpi_tpcases.pack(side="left", padx=40)
        self.kpi_casesmin.pack(side="left", padx=40)
        self.kpi_cases_to_complete.pack(side="right", padx=40)

        row4 = tk.Frame(root, bg="#000000")
        row4.pack(fill="x", padx=30, pady=(0, 10))

        self.case_completed = RatioBlock(row4, "CASE RATIO - COMPLETED")
        self.case_remaining = RatioBlock(row4, "CASE RATIO - REMAINING")
        self.case_completed.pack(side="left", padx=60)
        self.case_remaining.pack(side="left", padx=60)

        sep = tk.Frame(root, bg="#00ff00", height=2)
        sep.pack(fill="x", padx=10, pady=(10, 10))

        lbl = tk.Label(root, text="ESTIMATED TIME OF COMPLETION", bg="#000000", fg="#00ff00",
                       font=("Segoe UI", 22, "bold"))
        lbl.pack(pady=(0, 8))

        self.lbl_estimates = tk.Label(root, text="Run Shortsheet + Production then Refresh Dashboard.",
                                      bg="#000000", fg="#00ff00", font=("Segoe UI", 12, "bold"),
                                      justify="left")
        self.lbl_estimates.pack(anchor="w", padx=60, pady=(0, 6))

        self.lbl_warning = tk.Label(root, text="", bg="#000000", fg="#ff2b2b",
                                    font=("Segoe UI", 12, "bold"), justify="left")
        self.lbl_warning.pack(anchor="w", padx=60, pady=(0, 10))

        self.refresh_dashboard()

    def open_category_dashboard(self, fed_value: str):
        def compute():
            return self.compute_dashboard_payload(fed_filter=fed_value)
        CategoryDashboard(self, f"{fed_value} Dashboard", compute)

    def compute_dashboard_payload(self, fed_filter: str | None = None) -> dict:
        """
        MAIN page (fed_filter None):
          - keep overall trays/min KPI
          - show trays/min by machine inside estimate block (OSSID + REPAK)
          - show projected finish clock times (like before)

        FED popups (fed_filter set):
          - DO NOT show "finish at 14:32" (because we may not run that category continuously)
          - Instead show only: "Hours needed at current overall rate" + "Hours needed at standard"
          - (still broken down by machine, using OVERALL machine rates)
        """
        now = datetime.now()

        # --- Compute run mins from manual start time ---
        run_mins = 0.0
        start_dt = None
        end_dt = None
        try:
            st = parse_hhmm(self.var_start_time.get())
            et = parse_hhmm(self.var_end_time.get())
            start_dt = datetime.combine(now.date(), st)
            end_dt = end_datetime_for_today(start_dt, et)
            run_mins = minutes_between(start_dt, now)
        except Exception:
            run_mins = 0.0

        # --- Production totals (overall and optionally category-filtered for "completed" KPIs) ---
        prod_df = self.last_production_df

        trays_completed_total = 0.0
        cases_completed_total = 0.0
        trays_completed_ossid = 0.0
        trays_completed_repak = 0.0
        cases_completed_ossid = 0.0
        cases_completed_repak = 0.0

        # Always compute ALL for rates
        trays_completed_total_all = 0.0
        trays_completed_ossid_all = 0.0
        trays_completed_repak_all = 0.0

        if prod_df is not None and not prod_df.empty:
            p_all = prod_df.copy()
            trays_completed_total_all = float(p_all.get("TraysProduced", 0).sum())
            po_all = p_all[p_all.get("Machine", "").astype(str).str.strip() == MACHINE_OSSID]
            pr_all = p_all[p_all.get("Machine", "").astype(str).str.strip() == MACHINE_REPAK]
            trays_completed_ossid_all = float(po_all.get("TraysProduced", 0).sum()) if not po_all.empty else 0.0
            trays_completed_repak_all = float(pr_all.get("TraysProduced", 0).sum()) if not pr_all.empty else 0.0

            p = p_all
            if fed_filter:
                p = p[p.get("Fed", "").fillna("").astype(str).str.strip() == fed_filter].copy()

            trays_completed_total = float(p.get("TraysProduced", 0).sum())
            cases_completed_total = float(p.get("CasesProduced", 0).sum())

            po = p[p.get("Machine", "").astype(str).str.strip() == MACHINE_OSSID]
            pr = p[p.get("Machine", "").astype(str).str.strip() == MACHINE_REPAK]
            trays_completed_ossid = float(po.get("TraysProduced", 0).sum()) if not po.empty else 0.0
            trays_completed_repak = float(pr.get("TraysProduced", 0).sum()) if not pr.empty else 0.0
            cases_completed_ossid = float(po.get("CasesProduced", 0).sum()) if not po.empty else 0.0
            cases_completed_repak = float(pr.get("CasesProduced", 0).sum()) if not pr.empty else 0.0

        # Actual rates (overall + machine) based on ALL production
        trays_per_min_all = (trays_completed_total_all / run_mins) if run_mins > 0 else 0.0
        trays_per_min_ossid_all = (trays_completed_ossid_all / run_mins) if run_mins > 0 else 0.0
        trays_per_min_repak_all = (trays_completed_repak_all / run_mins) if run_mins > 0 else 0.0

        trays_per_min_display = (trays_completed_total / run_mins) if run_mins > 0 else 0.0
        cases_per_min_display = (cases_completed_total / run_mins) if run_mins > 0 else 0.0

        # --- Remaining (shortsheet) ---
        trays_remaining_total = 0.0
        cases_remaining_total = 0.0
        trays_remaining_ossid = 0.0
        trays_remaining_repak = 0.0
        cases_remaining_ossid = 0.0
        cases_remaining_repak = 0.0

        planned_cases = 0.0
        planned_trays = 0.0

        remain_for_std = pd.DataFrame()

        ss_detail = self.last_shortsheets_detail
        if ss_detail is not None and not ss_detail.empty:
            ss = ss_detail.copy()
            ss["Fed"] = ss.get("Fed", "").fillna("").astype(str).str.strip()
            if fed_filter:
                ss = ss[ss["Fed"] == fed_filter].copy()

            ss["TraysPerCase"] = pd.to_numeric(ss.get("TraysPerCase", 0), errors="coerce").fillna(0).astype(float)
            ss["StdTPM"] = pd.to_numeric(ss.get("StdTPM", 0), errors="coerce").fillna(0).astype(float)
            ss["RemainingCases"] = pd.to_numeric(ss.get("RemainingCases", 0), errors="coerce").fillna(0).astype(float)

            if "Machine" not in ss.columns:
                ss["Machine"] = "Unknown"
            ss["Machine"] = ss["Machine"].fillna("Unknown").astype(str).str.strip()

            if "Excluded" not in ss.columns:
                ss["Excluded"] = False
            ss["Excluded"] = ss["Excluded"].astype(bool)

            ss["TraysRemaining"] = ss["RemainingCases"] * ss["TraysPerCase"]

            planned_df = ss[ss["Excluded"]].copy()
            planned_cases = float(planned_df["RemainingCases"].sum()) if not planned_df.empty else 0.0
            planned_trays = float(planned_df["TraysRemaining"].sum()) if not planned_df.empty else 0.0

            if bool(self.var_apply_planned_shorts.get()):
                ss_calc = ss[~ss["Excluded"]].copy()
            else:
                ss_calc = ss.copy()

            trays_remaining_total = float(ss_calc["TraysRemaining"].sum())
            cases_remaining_total = float(ss_calc["RemainingCases"].sum())

            so = ss_calc[ss_calc["Machine"] == MACHINE_OSSID]
            sr = ss_calc[ss_calc["Machine"] == MACHINE_REPAK]

            trays_remaining_ossid = float(so["TraysRemaining"].sum()) if not so.empty else 0.0
            trays_remaining_repak = float(sr["TraysRemaining"].sum()) if not sr.empty else 0.0
            cases_remaining_ossid = float(so["RemainingCases"].sum()) if not so.empty else 0.0
            cases_remaining_repak = float(sr["RemainingCases"].sum()) if not sr.empty else 0.0

            remain_for_std = ss_calc[["Machine", "TraysRemaining", "StdTPM"]].copy()

        # --- Standard minutes (by machine + total) ---
        std = self.compute_standard_minutes(remain_for_std)
        std_ossid_min = std["ossid"]
        std_repak_min = std["repak"]
        std_total_min = std["total"]

        # --- Build estimates (REVERT STYLE FOR MAIN; HOURS-ONLY FOR FED) ---
        lines = []
        warn_lines = []

        if fed_filter:
            # FED POPUP: show "hours needed" only
            lines.append(f"Rate basis: OVERALL performance (not category-specific)")
            lines.append("")
            lines.append(f"CURRENT RATE (OSSID): {trays_per_min_ossid_all:,.2f} trays/min")
            lines.append(f"CURRENT RATE (REPAK): {trays_per_min_repak_all:,.2f} trays/min")
            lines.append(f"CURRENT RATE (TOTAL): {trays_per_min_all:,.2f} trays/min")
            lines.append("")

            def hours_needed(rem_trays: float, rate: float) -> str:
                if rem_trays <= 0:
                    return "0 min"
                if rate <= 0:
                    return "Need Production + Start time"
                return fmt_duration_minutes(rem_trays / rate)

            lines.append(f"OSSID remaining trays: {trays_remaining_ossid:,.0f} -> {hours_needed(trays_remaining_ossid, trays_per_min_ossid_all)}")
            lines.append(f"REPAK remaining trays: {trays_remaining_repak:,.0f} -> {hours_needed(trays_remaining_repak, trays_per_min_repak_all)}")
            lines.append(f"TOTAL remaining trays: {trays_remaining_total:,.0f} -> {hours_needed(trays_remaining_total, trays_per_min_all)}")
            lines.append("")
            lines.append("STANDARD (based on StdTPM + lines running):")
            lines.append(f"OSSID standard time: {fmt_duration_minutes(std_ossid_min)}")
            lines.append(f"REPAK standard time: {fmt_duration_minutes(std_repak_min)}")
            lines.append(f"TOTAL standard time: {fmt_duration_minutes(std_total_min)}")

        else:
            # MAIN PAGE: show finish times + include trays/min by machine (the “fun stuff”)
            end_txt = end_dt.strftime("%H:%M") if end_dt else "—"
            start_txt = start_dt.strftime("%H:%M") if start_dt else "—"
            lines.append(f"Start: {start_txt} | End: {end_txt} | Run mins so far: {run_mins:,.0f}")
            lines.append(f"Trays/min (TOTAL): {trays_per_min_all:,.2f} | OSSID: {trays_per_min_ossid_all:,.2f} | REPAK: {trays_per_min_repak_all:,.2f}")
            lines.append("")

            def fmt_finish(machine_name: str, rem_trays: float, rate: float, std_min: float):
                if rem_trays <= 0:
                    return f"{machine_name}: no remaining trays."
                if rate <= 0:
                    return f"{machine_name}: need Production + Start time to estimate."
                mins_needed = rem_trays / rate
                finish = now + timedelta(minutes=mins_needed)
                return (
                    f"{machine_name}: Remaining {rem_trays:,.0f} trays | "
                    f"Est {fmt_duration_minutes(mins_needed)} | Finish {finish.strftime('%H:%M')} | "
                    f"Standard {fmt_duration_minutes(std_min)}"
                )

            lines.append(fmt_finish("OSSID", trays_remaining_ossid, trays_per_min_ossid_all, std_ossid_min))
            lines.append(fmt_finish("REPAK", trays_remaining_repak, trays_per_min_repak_all, std_repak_min))
            lines.append(fmt_finish("TOTAL", trays_remaining_total, trays_per_min_all, std_total_min))

            # Warnings vs end time (use ACTUAL)
            if start_dt is not None and end_dt is not None:
                if trays_remaining_ossid > 0 and trays_per_min_ossid_all > 0:
                    finish_o = now + timedelta(minutes=(trays_remaining_ossid / trays_per_min_ossid_all))
                    if finish_o > end_dt:
                        warn_lines.append(f"⚠ OSSID projected finish {finish_o.strftime('%H:%M')} is after END TIME {end_dt.strftime('%H:%M')}")
                if trays_remaining_repak > 0 and trays_per_min_repak_all > 0:
                    finish_r = now + timedelta(minutes=(trays_remaining_repak / trays_per_min_repak_all))
                    if finish_r > end_dt:
                        warn_lines.append(f"⚠ REPAK projected finish {finish_r.strftime('%H:%M')} is after END TIME {end_dt.strftime('%H:%M')}")
                if trays_remaining_total > 0 and trays_per_min_all > 0:
                    finish_t = now + timedelta(minutes=(trays_remaining_total / trays_per_min_all))
                    if finish_t > end_dt:
                        warn_lines.append(f"⚠ TOTAL projected finish {finish_t.strftime('%H:%M')} is after END TIME {end_dt.strftime('%H:%M')}")

        # --- Render payload ---
        def fmt_kpi(v, decimals=0):
            if v <= 0:
                return "—"
            return f"{v:,.{decimals}f}"

        planned_txt = "—"
        if planned_cases > 0 or planned_trays > 0:
            planned_txt = f"{planned_cases:,.0f} cs\n{planned_trays:,.0f} tr"

        return {
            "tray_pack": fmt_kpi(trays_completed_total, 0),
            "trays_min": fmt_kpi(trays_per_min_all, 2) if trays_per_min_all > 0 else "—",
            "trays_to_complete": fmt_kpi(trays_remaining_total, 0),
            "planned": planned_txt,
            "trays_done_o": fmt_kpi(trays_completed_ossid, 0),
            "trays_done_r": fmt_kpi(trays_completed_repak, 0),
            "trays_rem_o": fmt_kpi(trays_remaining_ossid, 0),
            "trays_rem_r": fmt_kpi(trays_remaining_repak, 0),
            "tp_cases": fmt_kpi(cases_completed_total, 0),
            "cases_min": fmt_kpi(cases_per_min_display, 2) if (cases_per_min_display > 0 and not fed_filter) else ("—" if fed_filter else "—"),
            "cases_to_complete": fmt_kpi(cases_remaining_total, 0),
            "cases_done_o": fmt_kpi(cases_completed_ossid, 0),
            "cases_done_r": fmt_kpi(cases_completed_repak, 0),
            "cases_rem_o": fmt_kpi(cases_remaining_ossid, 0),
            "cases_rem_r": fmt_kpi(cases_remaining_repak, 0),
            "estimates": "\n".join(lines),
            "warnings": "\n".join(warn_lines) if warn_lines else ""
        }

    def refresh_dashboard(self):
        now = datetime.now()
        self.lbl_dash_time.configure(text=now.strftime("%m/%d/%Y %H:%M"))
        payload = self.compute_dashboard_payload(fed_filter=None)
        if not payload:
            return

        self.kpi_traypack.set_value(payload["tray_pack"])
        self.kpi_traysmin.set_value(payload["trays_min"])  # stays on main
        self.kpi_trays_to_complete.set_value(payload["trays_to_complete"])
        self.kpi_planned.set_value(payload["planned"])

        self.tray_completed.set_values(payload["trays_done_o"], payload["trays_done_r"])
        self.tray_remaining.set_values(payload["trays_rem_o"], payload["trays_rem_r"])
        self.case_completed.set_values(payload["cases_done_o"], payload["cases_done_r"])
        self.case_remaining.set_values(payload["cases_rem_o"], payload["cases_rem_r"])

        self.kpi_tpcases.set_value(payload["tp_cases"])
        self.kpi_casesmin.set_value(payload["cases_min"])
        self.kpi_cases_to_complete.set_value(payload["cases_to_complete"])

        self.lbl_estimates.configure(text=payload["estimates"])
        self.lbl_warning.configure(text=payload["warnings"])

    # ---------------- Planned shorts actions ----------------
    def on_planned_search(self):
        q = (self.var_planned_search.get() or "").strip().lower()
        if not q:
            return

        def pred(row):
            plu = str(row.get("PLU", "")).lower()
            desc = str(row.get("ProductDescription", "")).lower()
            fed = str(row.get("Fed", "")).lower()
            return (q in plu) or (q in desc) or (q in fed)

        found = self.short_table.select_first_match(pred)
        if not found:
            messagebox.showinfo("Search", "No match found.")

    def on_planned_exclude_selected(self):
        row = self.short_table.get_selected_values()
        plu = norm_plu(row.get("PLU", ""))
        if not plu:
            messagebox.showinfo("Exclude", "Select a row (PLU) to exclude.")
            return
        key = self._range_key()
        planned = self._get_planned_set_for_range(key)
        planned.add(plu)
        self._set_planned_for_range(key, planned)
        self._refresh_planned_listbox()

    def on_planned_remove_from_list(self):
        sel = self.lst_planned.curselection()
        if not sel:
            return
        plu = norm_plu(self.lst_planned.get(sel[0]))
        key = self._range_key()
        planned = self._get_planned_set_for_range(key)
        if plu in planned:
            planned.remove(plu)
            self._set_planned_for_range(key, planned)
            self._refresh_planned_listbox()

    def on_planned_bulk_apply(self):
        raw = (self.var_planned_bulk.get() or "").strip()
        if not raw:
            return
        tokens = []
        for part in raw.replace(",", " ").replace("\n", " ").replace("\t", " ").split():
            p = norm_plu(part)
            if p:
                tokens.append(p)
        if not tokens:
            messagebox.showinfo("Bulk", "No valid PLUs found in the bulk list.")
            return
        key = self._range_key()
        planned = self._get_planned_set_for_range(key)
        planned.update(tokens)
        self._set_planned_for_range(key, planned)
        self._refresh_planned_listbox()

    # ---------------- Shortsheet actions ----------------
    def on_run_shortsheets(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed.")
            return

        self._load_master(initial=True)

        try:
            from_d, to_d = self._parse_txn_from_to()
        except ValueError:
            messagebox.showerror("TxnDate Range", "Enter dates as YYYY-MM-DD.")
            return

        statuses = self._get_short_wip_statuses()
        only_remaining = bool(self.var_only_remaining.get())
        exclude_missing = bool(self.cfg.exclude_missing_master)
        exclude_frozen = bool(self.cfg.exclude_frozen_y)

        try:
            self._log("Connecting to SQL Server (shortsheet)...")
            with pyodbc.connect(build_connection_string(self.cfg), timeout=25) as conn:
                df = fetch_shortsheets_range(conn, from_d, to_d, statuses, only_remaining)

            if df.empty:
                self._log("Shortsheet returned 0 rows.")
                self.last_shortsheets_df = df
                self.last_shortsheets_detail = df
                self.short_table.set_dataframe(pd.DataFrame(columns=self.short_table.columns))
                self.refresh_dashboard()
                messagebox.showinfo("Shortsheet", "No remaining balances found.")
                return

            df2 = df.copy()

            # Join master
            if self.master_df is not None and not self.master_df.empty:
                m = self.master_df.copy()
                df2["PLU"] = df2["PLU"].astype(str).str.zfill(5)
                df2 = df2.merge(m[["PLU", "DESC", "Trays", "TPM", "Type", "Machine", "Frozen", "Fed"]],
                                on="PLU", how="left")
                df2 = df2.rename(columns={"DESC": "ProductDescription", "Trays": "TraysPerCase", "TPM": "StdTPM"})
            else:
                df2["ProductDescription"] = ""
                df2["TraysPerCase"] = None
                df2["StdTPM"] = None
                df2["Type"] = ""
                df2["Machine"] = ""
                df2["Frozen"] = ""
                df2["Fed"] = ""

            df2["TraysPerCase"] = pd.to_numeric(df2["TraysPerCase"], errors="coerce")
            df2["StdTPM"] = pd.to_numeric(df2["StdTPM"], errors="coerce")
            df2["Machine"] = df2["Machine"].fillna("Unknown").astype(str).str.strip()
            df2["Frozen"] = df2["Frozen"].fillna("").astype(str).str.strip().str.upper()
            df2["Fed"] = df2["Fed"].fillna("").astype(str).str.strip()

            if exclude_missing and self.master_df is not None and not self.master_df.empty:
                df2 = df2[df2["TraysPerCase"].notna()].copy()

            if exclude_frozen:
                df2 = df2[df2["Frozen"] != "Y"].copy()

            # Planned shorts flag (by RANGE)
            key = self._range_key()
            planned_set = self._get_planned_set_for_range(key)
            df2["Excluded"] = df2["PLU"].astype(str).str.zfill(5).isin(planned_set)

            # Sort by Fed custom order then RemainingCases desc
            fed_cat = pd.Categorical(df2["Fed"], categories=FED_ORDER, ordered=True)
            df2["_FedSort"] = fed_cat
            df2 = df2.sort_values(by=["_FedSort", "RemainingCases", "Fed", "PLU"], ascending=[True, False, True, True])
            df2 = df2.drop(columns=["_FedSort"])

            self.last_shortsheets_detail = df2[[
                "Fed", "Machine", "Type",
                "ProductNumber", "PLU", "ProductDescription", "QtyOrdered", "QtyShipped", "AvailableCases",
                "RemainingCases", "TraysPerCase", "StdTPM", "Frozen", "Excluded"
            ]].copy()

            display = df2[[
                "Fed", "Machine",
                "ProductNumber", "PLU", "ProductDescription", "QtyOrdered", "QtyShipped",
                "AvailableCases", "RemainingCases", "Excluded"
            ]].copy()

            self.last_shortsheets_df = display
            self.short_table.set_dataframe(display)
            self._log(f"Shortsheet rows: {len(display):,} (TxnDate range {from_d} -> {to_d})")

            self._refresh_planned_listbox()
            self.refresh_dashboard()
        except Exception as e:
            self._log(f"ERROR shortsheet: {e}")
            self._log(traceback.format_exc())
            messagebox.showerror("Shortsheet Error", str(e))

    # ---------------- Production ----------------
    def on_refresh_production(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed.")
            return

        self._load_master(initial=True)
        if self.master_df is None or self.master_df.empty:
            messagebox.showerror("Product Master", "Load Product Info.xlsx first (Settings).")
            return

        pack = (self.var_packdate.get() or "").strip()
        if not pack:
            messagebox.showerror("PackDate", "Enter the 4-digit packdate code (e.g., 1307).")
            return

        pack_param = int(pack) if pack.isdigit() else pack
        machine_filter = (self.var_machine.get() or "All").strip()

        try:
            parse_hhmm(self.var_start_time.get())
            parse_hhmm(self.var_end_time.get())
        except Exception:
            messagebox.showerror("Time", "Enter Start/End time as HH:MM (24-hour), e.g., 08:30")
            return

        try:
            self._log("Connecting to SQL Server (production)...")
            with pyodbc.connect(build_connection_string(self.cfg), timeout=25) as conn:
                prod_df = fetch_production_by_packdate(conn, pack_param, self.production_statuses)

            if prod_df.empty:
                self._log("Production query returned 0 rows.")
                self.last_production_df = pd.DataFrame()
                self.prod_table.set_dataframe(pd.DataFrame(columns=self.prod_table.columns))
                self.refresh_dashboard()
                messagebox.showinfo("Production", "No production found for that packdate/status set.")
                return

            m = self.master_df.copy()
            prod_df["PLU"] = prod_df["PLU"].astype(str).str.zfill(5)
            merged = prod_df.merge(m[["PLU", "DESC", "Trays", "TPM", "Type", "Machine", "Frozen", "Fed"]], on="PLU", how="left")

            merged = merged.rename(columns={"Trays": "TraysPerCase", "TPM": "StdTPM"})
            merged["TraysPerCase"] = pd.to_numeric(merged["TraysPerCase"], errors="coerce").fillna(0).astype(float)
            merged["StdTPM"] = pd.to_numeric(merged["StdTPM"], errors="coerce").fillna(0).astype(float)
            merged["CasesProduced"] = pd.to_numeric(merged["CasesProduced"], errors="coerce").fillna(0).astype(float)
            merged["LbsProduced"] = pd.to_numeric(merged["LbsProduced"], errors="coerce").fillna(0).astype(float)

            if self.cfg.exclude_missing_master:
                merged = merged[merged["TraysPerCase"] > 0].copy()

            if self.cfg.exclude_frozen_y:
                merged["Frozen"] = merged["Frozen"].astype(str).str.strip().str.upper()
                merged = merged[merged["Frozen"] != "Y"].copy()

            merged["Machine"] = merged["Machine"].fillna("Unknown").astype(str).str.strip()
            merged["Type"] = merged["Type"].fillna("Unknown")
            merged["Fed"] = merged["Fed"].fillna("").astype(str).str.strip()
            merged["DESC"] = merged["DESC"].fillna("")

            if machine_filter != "All":
                merged = merged[merged["Machine"].astype(str).str.strip() == machine_filter].copy()

            merged["TraysProduced"] = merged["CasesProduced"] * merged["TraysPerCase"]
            self.last_production_df = merged.copy()

            merged_display = merged[[
                "Fed", "Machine", "Type", "ProductNumber", "PLU", "DESC", "TraysPerCase", "StdTPM",
                "CasesProduced", "LbsProduced", "TraysProduced"
            ]].copy().sort_values(by=["Fed", "Machine", "Type", "TraysProduced"], ascending=[True, True, True, False])

            self.prod_table.set_dataframe(merged_display)

            self._log(f"Production rows (PLU): {len(merged_display):,}")
            self.refresh_dashboard()
        except Exception as e:
            self._log(f"ERROR production: {e}")
            self._log(traceback.format_exc())
            messagebox.showerror("Production Error", str(e))

    # ---------------- Tabs ----------------
    def _build_tab_shortsheets(self):
        pad = {"padx": 10, "pady": 6}

        frm = ttk.LabelFrame(self.tab_short, text="Inputs")
        frm.pack(fill="x", **pad)

        r = 0
        ttk.Label(frm, text="TxnDate From (YYYY-MM-DD):").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_txn_from, width=16).grid(row=r, column=1, sticky="w", **pad)

        ttk.Label(frm, text="TxnDate To (YYYY-MM-DD):").grid(row=r, column=2, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_txn_to, width=16).grid(row=r, column=3, sticky="w", **pad)

        ttk.Checkbutton(frm, text="Only Remaining (>0)", variable=self.var_only_remaining).grid(row=r, column=4, sticky="w", **pad)
        ttk.Checkbutton(frm, text="Apply Planned Shorts (exclude from totals/ETA)", variable=self.var_apply_planned_shorts,
                        command=self.refresh_dashboard).grid(row=r, column=5, sticky="w", **pad)

        r += 1
        ttk.Label(frm, text="WIP statuses counted as AvailableCases:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Checkbutton(frm, text="Available", variable=self.var_wip_available).grid(row=r, column=1, sticky="w", **pad)
        ttk.Checkbutton(frm, text="ScanningSalesOrder", variable=self.var_wip_scanning).grid(row=r, column=2, sticky="w", **pad)
        ttk.Checkbutton(frm, text="WaitingToBeInvoiced", variable=self.var_wip_waiting).grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Button(frm, text="Run Shortsheet", command=self.on_run_shortsheets).grid(row=r, column=0, sticky="w", **pad)

        planned = ttk.LabelFrame(self.tab_short, text="Planned Shorts (Exclude specific PLUs from totals/ETA)")
        planned.pack(fill="x", padx=10, pady=(0, 8))

        ppad = {"padx": 10, "pady": 6}
        ttk.Label(planned, text="Search (PLU / Desc / Fed):").grid(row=0, column=0, sticky="w", **ppad)
        ttk.Entry(planned, textvariable=self.var_planned_search, width=28).grid(row=0, column=1, sticky="w", **ppad)
        ttk.Button(planned, text="Find", command=self.on_planned_search).grid(row=0, column=2, sticky="w", **ppad)

        ttk.Button(planned, text="Exclude Selected PLU", command=self.on_planned_exclude_selected).grid(row=0, column=3, sticky="w", **ppad)

        ttk.Label(planned, text="Bulk add PLUs:").grid(row=1, column=0, sticky="w", **ppad)
        ttk.Entry(planned, textvariable=self.var_planned_bulk, width=50).grid(row=1, column=1, columnspan=2, sticky="w", **ppad)
        ttk.Button(planned, text="Apply Bulk", command=self.on_planned_bulk_apply).grid(row=1, column=3, sticky="w", **ppad)

        ttk.Label(planned, text="Excluded PLUs (Range):").grid(row=0, column=4, sticky="w", **ppad)
        self.lst_planned = tk.Listbox(planned, height=4, width=16)
        self.lst_planned.grid(row=0, column=5, rowspan=2, sticky="w", padx=10, pady=6)
        ttk.Button(planned, text="Remove Selected", command=self.on_planned_remove_from_list).grid(row=0, column=6, sticky="w", **ppad)

        self.short_table = TableView(self.tab_short, columns=[
            "Fed", "Machine",
            "ProductNumber", "PLU", "ProductDescription", "QtyOrdered", "QtyShipped",
            "AvailableCases", "RemainingCases", "Excluded"
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

        ttk.Label(frm, text="Ossid Lines:").grid(row=r, column=8, sticky="w", **pad)
        ttk.Spinbox(frm, from_=1, to=7, textvariable=self.var_ossid_lines, width=5).grid(row=r, column=9, sticky="w", **pad)

        ttk.Label(frm, text="Repak Lines:").grid(row=r, column=10, sticky="w", **pad)
        ttk.Spinbox(frm, from_=1, to=7, textvariable=self.var_repak_lines, width=5).grid(row=r, column=11, sticky="w", **pad)

        r += 1
        ttk.Button(frm, text="Refresh Production", command=self.on_refresh_production).grid(row=r, column=0, sticky="w", **pad)

        self.prod_table = TableView(self.tab_prod, columns=[
            "Fed", "Machine", "Type", "ProductNumber", "PLU", "DESC", "TraysPerCase", "StdTPM",
            "CasesProduced", "LbsProduced", "TraysProduced"
        ], height=18)
        self.prod_table.pack(fill="both", expand=True, padx=10, pady=10)


def main():
    App().mainloop()


if __name__ == "__main__":
    main()
