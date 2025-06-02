import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import dask.dataframe as dd
import threading
import os
import time

class DataAnalyzerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("泰澐數據分拆工具")

        self.root.geometry("1200x800") # 調整視窗大小以容納更多內容
        self.root.resizable(True, True)

        self.df = None
        self.ddf = None
        self.file_path = None
        self.selected_columns_prioritized = [] # 儲存 [column_name1, column_name2] (已排序)
        self.all_columns_display = []
        self.all_unique_values_display = []
        self.final_export_columns = [] # 新增：儲存最終匯出時的欄位列表

        self.search_entry_var = tk.StringVar()
        # 儲存 {column_name: {'priority_var': tk.StringVar, 'filter_var': tk.BooleanVar, 'priority_entry_widget': ttk.Entry}}
        self.column_selection_vars_and_widgets = {} 
        self.column_search_entry_var = tk.StringVar()

        # 新增變數用於儲存已套用的篩選資訊
        # 格式: {'column_name': [list_of_selected_values] 或 'SKIP_FILTER'}
        self.applied_filters_display_data = {} 
        self.current_filtered_df_or_ddf = None # 儲存每次篩選後的結果

        self.loading_messages = [
            "泰澐說：請深呼吸，準備處理大量數據...",
            "泰澐說：數據的海洋正在為您展開，請稍候！",
            "泰澐說：咖啡沖泡中... 數據處理也正在加速！",
            "泰澐說：小精靈們正在努力收集唯一值，請耐心等待...",
            "泰澐說：這是Dask的魔力！讓大型數據不再可怕...",
            "泰澐說：您的數據正在被仔細檢查，就像尋找寶藏一樣！",
            "泰澐說：幾乎完成了！距離洞察只差一步...",
            "泰澐說：如果檔案很大，這可能需要一點時間。請耐心等候！",
            "泰澐說：正在為您量身定制篩選選項...",
            "泰澐說：數據是新的黃金，我們正在為您提煉中...",
            "泰澐說：系統正在思考中，請勿打擾...",
            "泰澐說：正在校準分析儀器，請稍候...",
            "泰澐說：數據傳輸中，請稍候。這比龜速還要快一點！",
            "泰澐說：我們的數據引擎正在全速運轉！",
            "泰澐說：正在整理數據點，像拼圖一樣。",
            "泰澐說：這可不是普通的加載，這是在創造奇蹟！",
            "泰澐說：為您準備最精準的分析結果，請耐心。",
            "泰澐說：數據的魔法正在發生，請稍候。",
            "泰澐說：正在掃描數據，尋找寶貴的見解。",
            "泰澐說：讓數據說話！我們正在為它準備舞台。",
            "泰澐說：數據加工廠正在加班加點為您服務。",
            "泰澐說：別擔心，我們保證您的數據安全。",
            "泰澐說：數據的秘密即將揭曉，請稍候。",
            "泰澐說：耐心是美德，特別是當處理大數據時。",
            "泰澐說：我們正在確保每一個數據點都完美無缺。",
            "泰澐說：數據準備就緒，就像超級英雄一樣！",
            "泰澐說：系統正在充電中，很快就能為您提供服務。",
            "泰澐說：這是一場數據的交響樂，正在優雅地演奏。",
            "泰澐說：正在處理珍貴的數據，以最完美的姿態呈現。",
            "泰澐說：面對數據處理，是需要時間與智慧所淬煉的！",
            "泰澐說：就像探險家在叢林中尋找寶藏，我們正在數據中挖掘價值。",
            "泰澐說：一點點耐心，換來大大的方便。",
            "泰澐說：您的數據正在被溫柔地對待。",
            "泰澐說：我們正在讓複雜的數據變得簡單。",
            "泰澐說：別眨眼，可能很快就完成了！",
            "泰澐說：數據的宇宙正在為您展開新的篇章。",
            "泰澐說：正在準備您的數據盛宴。",
            "泰澐說：這不是卡住了，這是在思考人生...不，是數據！",
            "泰澐說：正在為您計算所有的可能性。",
            "泰澐說：一步步，數據正在向您走來。",
            "泰澐說：感謝您的等待，我們正在為您創造價值。",
            "泰澐說：數據正在經歷一次華麗的變身。",
            "泰澐說：讓等待也充滿期待！",
            "泰澐說：您的數據正在接受專業的SPA護理。",
            "泰澐說：正在整理數據的衣櫥，讓它煥然一新。",
            "泰澐說：我們正在讓數據跳舞，準備精彩的表演。",
            "泰澐說：這是一個值得等待的結果！",
            "泰澐說：數據正在為您準備一場視覺盛宴。",
            "泰澐說：請稍候，魔法即將發生。",
            "泰澐說：正在為您連接數據的脈絡。",
            "泰澐說：耐心等待，驚喜就在眼前。",
            "泰澐說：讓數據的力量為您所用。",
            "泰澐說：這是一個智慧的等待，因為它將帶來答案。"
        ]
        self.current_message_index = 0
        self.loading_animation_id_general = None
        self.loading_animation_id_process = None

        self.start_time = {}
        self.end_time = {}

        self._setup_style()
        self._create_widgets()

    def _setup_style(self):
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except tk.TclError:
            style.theme_use('alt')

        style.configure('TFrame', background='#e0f2f7')
        style.configure('TLabel', background='#e0f2f7', font=('微軟正黑體', 10))
        style.configure('TLabelframe', background='#e0f2f7', foreground='#004d40', font=('微軟正黑體', 11, 'bold'))
        style.configure('TLabelframe.Label', background='#e0f2f7', foreground='#004d40', font=('微軟正黑體', 11, 'bold'))

        style.configure('TButton',
                        font=('微軟正黑體', 10, 'bold'),
                        background='#4CAF50',
                        foreground='white',
                        padding=8)
        style.map('TButton',
                  background=[('active', '#66BB6A')],
                  foreground=[('active', 'white')])

        style.configure('TCheckbutton', background='#e0f2f7', font=('微軟正黑體', 10))
        style.configure('Vertical.TScrollbar', background='#81C784', troughcolor='#e0f2f7')
        style.configure('Treeview', font=('微軟正黑體', 9), rowheight=25)
        style.configure('Treeview.Heading', font=('微軟正黑體', 10, 'bold'))
        # 調整 Entry 的樣式
        style.configure('TEntry', fieldbackground='white', foreground='black', font=('微軟正黑體', 10))


    def _create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="20 20 20 20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        top_frame = ttk.Frame(main_frame, style='TFrame')
        top_frame.pack(fill=tk.X, pady=5)

        file_selection_frame = ttk.LabelFrame(top_frame, text="🟢 步驟 1: 選擇數據檔案", padding="15 15 15 15")
        file_selection_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        self.file_label = ttk.Label(file_selection_frame, text="未選擇檔案", font=('微軟正黑體', 10, 'italic'))
        self.file_label.pack(side=tk.LEFT, padx=10, expand=True, fill=tk.X)

        select_file_button = ttk.Button(file_selection_frame, text="📂 選擇檔案", command=self._select_file)
        select_file_button.pack(side=tk.RIGHT, padx=10)

        time_display_frame = ttk.LabelFrame(top_frame, text="⏱️ 執行時間紀錄", padding="15 15 15 15")
        time_display_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=False)
        self.time_text = tk.Text(time_display_frame, height=5, width=30, font=('微軟正黑體', 9), wrap=tk.WORD, state='disabled')
        self.time_text.pack(padx=5, pady=5)
        self._update_time_display("應用程式啟動", 0)

        self.current_content_frame = ttk.Frame(main_frame, style='TFrame')
        self.current_content_frame.pack(fill=tk.BOTH, expand=True)

        self._create_column_selection_widgets(self.current_content_frame)

    def _create_column_selection_widgets(self, parent_frame):
        for widget in parent_frame.winfo_children():
            widget.destroy()

        column_selection_main_frame = ttk.Frame(parent_frame, padding="20 20 20 20")
        column_selection_main_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(column_selection_main_frame, text="🎯 步驟 2: 選擇要分析的欄位",
                  font=('微軟正黑體', 12, 'bold'), background='#e0f2f7').pack(pady=10)
        ttk.Label(column_selection_main_frame, text="請為需要篩選的欄位設定優先順序（數字越小優先順序越高），並勾選「是否篩選」。",
                  font=('微軟正黑體', 9), background='#e0f2f7', foreground='gray').pack(pady=(0, 10))


        control_frame = ttk.Frame(column_selection_main_frame, style='TFrame')
        control_frame.pack(fill=tk.X, pady=(0, 5), padx=5)

        ttk.Button(control_frame, text="🔢 預設優先順序", command=lambda: self._set_default_priorities()).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="🧹 清空優先順序", command=lambda: self._clear_priorities()).pack(side=tk.LEFT, padx=5)
        
        # 新增全選/全不選「是否篩選」的按鈕
        ttk.Button(control_frame, text="☑️ 全選篩選", command=lambda: self._set_all_filter_checkboxes(True)).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="⬜ 全不選篩選", command=lambda: self._set_all_filter_checkboxes(False)).pack(side=tk.LEFT, padx=5)


        self.column_search_entry_var.set("")
        self.column_search_entry_var.trace_add("write", self._filter_columns_by_keyword)
        column_search_entry = ttk.Entry(control_frame, textvariable=self.column_search_entry_var, width=30, font=('微軟正黑體', 10))
        column_search_entry.pack(side=tk.RIGHT, padx=5, fill=tk.X, expand=True)
        ttk.Label(control_frame, text="🔍 搜尋欄位:", background='#e0f2f7').pack(side=tk.RIGHT)

        column_list_frame = ttk.LabelFrame(column_selection_main_frame, text="設定欄位優先順序及是否篩選", padding="10")
        column_list_frame.pack(fill=tk.BOTH, expand=True, pady=10)

        self.column_canvas = tk.Canvas(column_list_frame, bg='#f0fbfd', highlightthickness=0)
        self.column_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.column_scrollbar = ttk.Scrollbar(column_list_frame, orient="vertical", command=self.column_canvas.yview)
        self.column_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.column_canvas.configure(yscrollcommand=self.column_scrollbar.set)
        # Bind the scroll wheel to the canvas for easier scrolling
        self.column_canvas.bind('<Configure>', lambda e: self.column_canvas.configure(scrollregion = self.column_canvas.bbox("all")))
        self.column_canvas.bind_all("<MouseWheel>", self._on_mouse_wheel_column_canvas) # For Windows/macOS
        self.column_canvas.bind_all("<Button-4>", self._on_mouse_wheel_column_canvas) # For Linux
        self.column_canvas.bind_all("<Button-5>", self._on_mouse_wheel_column_canvas) # For Linux


        self.column_frame = ttk.Frame(self.column_canvas, style='TFrame')
        self.column_canvas.create_window((0, 0), window=self.column_frame, anchor="nw")

        # 這裡將儲存 {column_name: {'priority_var': tk.StringVar, 'filter_var': tk.BooleanVar, 'priority_entry_widget': ttk.Entry}}
        self.column_selection_vars_and_widgets = {} 

        # 確保按鈕在滾動區域下方，且在主框架底部
        self.next_step_button = ttk.Button(column_selection_main_frame, text="▶️ 下一步：篩選值", command=self._go_to_value_filter, state=tk.DISABLED)
        self.next_step_button.pack(pady=10) 


    def _on_mouse_wheel_column_canvas(self, event):
        if event.delta: # Windows/macOS
            self.column_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        else: # Linux
            if event.num == 4:
                self.column_canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                self.column_canvas.yview_scroll(1, "units")


    def _update_time_display(self, step_name, duration=None):
        if hasattr(self, 'time_text') and self.time_text.winfo_exists():
            self.time_text.config(state='normal')
            if duration is not None:
                self.time_text.insert(tk.END, f"{step_name}: {duration:.2f} 秒\n")
            else:
                self.time_text.insert(tk.END, f"{step_name}...\n")
            self.time_text.see(tk.END)
            self.time_text.config(state='disabled')
        else:
            print(f"Warning: time_text widget does not exist when trying to update for '{step_name}'.")

    def _select_file(self):
        self.start_time['檔案選擇'] = time.time()
        self._update_time_display("檔案選擇中")
        filetypes = [("CSV files", "*.csv"), ("Excel files", "*.xlsx"), ("All files", "*.*")]
        self.file_path = filedialog.askopenfilename(filetypes=filetypes)

        if self.file_path:
            self.file_label.config(text=f"已選擇檔案: {self.file_path.split('/')[-1]}")
            try:
                self.start_time['檔案讀取'] = time.time()
                self._update_time_display("檔案讀取中")

                if self.file_path.lower().endswith('.csv'):
                    self.ddf = dd.read_csv(self.file_path)
                    columns = self.ddf.columns.tolist()
                    self.df = None
                elif self.file_path.lower().endswith(('.xlsx', '.xls')):
                    self.df = pd.read_excel(self.file_path)
                    columns = self.df.columns.tolist()
                    self.ddf = None
                else:
                    messagebox.showerror("錯誤", "不支援的檔案類型。請選擇 CSV 或 Excel 檔案。")
                    self._clear_column_selection()
                    self.end_time['檔案讀取'] = time.time()
                    self._update_time_display("檔案讀取失敗", self.end_time['檔案讀取'] - self.start_time['檔案讀取'])
                    return

                self._display_columns(columns)
                self.next_step_button.config(state=tk.NORMAL)
                self.end_time['檔案讀取'] = time.time()
                self._update_time_display("檔案讀取完成", self.end_time['檔案讀取'] - self.start_time['檔案讀取'])
                self.end_time['檔案選擇'] = time.time()
                self._update_time_display("檔案選擇與讀取總耗時", self.end_time['檔案選擇'] - self.start_time['檔案選擇'])
            except Exception as e:
                messagebox.showerror("讀取檔案錯誤", f"無法讀取檔案: {e}\n請檢查檔案格式是否正確。")
                self._clear_column_selection()
                self.file_label.config(text="未選擇檔案")
                self.next_step_button.config(state=tk.DISABLED)
                self.end_time['檔案讀取'] = time.time()
                self._update_time_display("檔案讀取失敗", self.end_time['檔案讀取'] - self.start_time['檔案讀取'])
                self.end_time['檔案選擇'] = time.time()
                self._update_time_display("檔案選擇與讀取總耗時", self.end_time['檔案選擇'] - self.start_time['檔案選擇'])
        else:
            self._clear_column_selection()
            self.file_label.config(text="未選擇檔案")
            self.next_step_button.config(state=tk.DISABLED)
            self.end_time['檔案選擇'] = time.time()
            self._update_time_display("檔案選擇取消", self.end_time['檔案選擇'] - self.start_time['檔案選擇'])

    def _display_columns(self, columns):
        self._clear_column_selection()
        self.all_columns_display = sorted(columns)

        row_num = 0
        for col in self.all_columns_display:
            priority_var = tk.StringVar(value="") 
            filter_var = tk.BooleanVar(value=False) # 預設不篩選

            # 使用 Grid 布局來排列欄位名稱、是否篩選和 Entry
            col_label = ttk.Label(self.column_frame, text=col, style='TLabel')
            col_label.grid(row=row_num, column=0, sticky="w", padx=5, pady=2)

            # 先放置優先順序 Entry
            priority_entry = ttk.Entry(self.column_frame, textvariable=priority_var, width=5, justify='center', style='TEntry')
            priority_entry.grid(row=row_num, column=1, sticky="e", padx=5, pady=2)
            
            # 再放置「是否篩選」核選框，並綁定命令來控制 Entry 啟用/禁用和清除值
            filter_checkbox = ttk.Checkbutton(self.column_frame, text="是否篩選", variable=filter_var, style='TCheckbutton',
                                              command=lambda cv=col, pv=priority_var, pe=priority_entry: self._toggle_priority_entry_state(cv, pv, pe))
            filter_checkbox.grid(row=row_num, column=2, sticky="w", padx=5, pady=2)

            # 添加輸入驗證，只允許數字或空值
            vcmd = (self.root.register(self._validate_priority_input), '%P')
            priority_entry.config(validate='key', validatecommand=vcmd)

            self.column_selection_vars_and_widgets[col] = {
                'priority_var': priority_var,
                'filter_var': filter_var,
                'priority_entry_widget': priority_entry
            }
            row_num += 1

        self.root.update_idletasks()
        self.column_canvas.config(scrollregion=self.column_canvas.bbox("all"))

    def _toggle_priority_entry_state(self, column_name, priority_var, priority_entry_widget):
        # 根據 filter_var 的狀態啟用或禁用 priority_entry
        # 如果勾選「是否篩選」，則啟用 Entry；否則禁用並清空值
        if self.column_selection_vars_and_widgets[column_name]['filter_var'].get():
            priority_entry_widget.config(state=tk.NORMAL)
        else:
            priority_entry_widget.config(state=tk.DISABLED)
            priority_var.set("") # 清空優先順序


    def _validate_priority_input(self, P):
        # 允許空字串或只包含數字
        if P.strip() == "" or P.isdigit():
            return True
        return False

    def _set_default_priorities(self):
        # 根據當前顯示的欄位設定預設優先順序
        visible_columns_info = []
        keyword = self.column_search_entry_var.get().lower()
        for col_name in self.all_columns_display:
            if keyword in col_name.lower():
                visible_columns_info.append(col_name)

        # 清空所有現有優先順序和篩選狀態
        self._clear_priorities()

        # 對所有可見欄位設定預設優先順序並勾選篩選
        for i, col_name in enumerate(visible_columns_info):
            self.column_selection_vars_and_widgets[col_name]['filter_var'].set(True)
            self.column_selection_vars_and_widgets[col_name]['priority_var'].set(str(i + 1))
            # 確保 Entry 被啟用
            self.column_selection_vars_and_widgets[col_name]['priority_entry_widget'].config(state=tk.NORMAL)


    def _clear_priorities(self):
        # 清空所有優先順序和篩選狀態
        for col_name, vars_widgets in self.column_selection_vars_and_widgets.items():
            vars_widgets['priority_var'].set("")
            vars_widgets['filter_var'].set(False)
            vars_widgets['priority_entry_widget'].config(state=tk.DISABLED) # 禁用 Entry

    def _set_all_filter_checkboxes(self, select_all):
        keyword = self.column_search_entry_var.get().lower()
        for col_name in self.all_columns_display:
            if keyword in col_name.lower():
                self.column_selection_vars_and_widgets[col_name]['filter_var'].set(select_all)
                # 同步更新 Entry 狀態
                self._toggle_priority_entry_state(
                    col_name,
                    self.column_selection_vars_and_widgets[col_name]['priority_var'],
                    self.column_selection_vars_and_widgets[col_name]['priority_entry_widget']
                )


    def _filter_columns_by_keyword(self, *args):
        keyword = self.column_search_entry_var.get().lower()

        for widget in self.column_frame.winfo_children():
            widget.destroy()

        row_num = 0
        for original_col in self.all_columns_display:
            if keyword in original_col.lower():
                col_info = self.column_selection_vars_and_widgets[original_col]
                priority_var = col_info['priority_var']
                filter_var = col_info['filter_var']
                
                col_label = ttk.Label(self.column_frame, text=original_col, style='TLabel')
                col_label.grid(row=row_num, column=0, sticky="w", padx=5, pady=2)

                # 重建 Entry，並設置正確的狀態
                priority_entry = ttk.Entry(self.column_frame, textvariable=priority_var, width=5, justify='center', style='TEntry', 
                                           state=tk.NORMAL if filter_var.get() else tk.DISABLED)
                priority_entry.grid(row=row_num, column=1, sticky="e", padx=5, pady=2)
                vcmd = (self.root.register(self._validate_priority_input), '%P')
                priority_entry.config(validate='key', validatecommand=vcmd)
                
                filter_checkbox = ttk.Checkbutton(self.column_frame, text="是否篩選", variable=filter_var, style='TCheckbutton',
                                                  command=lambda cv=original_col, pv=priority_var, pe=priority_entry: self._toggle_priority_entry_state(cv, pv, pe))
                filter_checkbox.grid(row=row_num, column=2, sticky="w", padx=5, pady=2)

                # 更新字典中的 widget 引用
                self.column_selection_vars_and_widgets[original_col]['priority_entry_widget'] = priority_entry

                row_num += 1

        self.root.update_idletasks()
        self.column_canvas.config(scrollregion=self.column_canvas.bbox("all"))

    def _clear_column_selection(self):
        for widget in self.column_frame.winfo_children():
            widget.destroy()
        self.column_selection_vars_and_widgets.clear()
        self.all_columns_display = []
        self.column_search_entry_var.set("")
        self.selected_columns_prioritized = []
        self.final_export_columns = [] # 清除最終匯出欄位列表

    def _go_to_value_filter(self):
        self.start_time['進入值篩選'] = time.time()
        self._update_time_display("進入值篩選介面")

        selected_for_filter_with_priorities = []
        # 新增：收集所有勾選了「是否篩選」的欄位，無論是否有設定優先順序
        # 這將是最終匯出的欄位集合
        self.final_export_columns = [] 
        for col_name, info in self.column_selection_vars_and_widgets.items():
            if info['filter_var'].get(): # 只要勾選了「是否篩選」的欄位就加入
                self.final_export_columns.append(col_name)

                priority_str = info['priority_var'].get().strip()
                if not priority_str:
                    # 如果勾選了篩選但沒有設定優先順序，給予警告
                    messagebox.showwarning("輸入錯誤", f"欄位 '{col_name}' 已勾選篩選，但未設定優先順序。請為所有勾選的篩選欄位設定優先順序。")
                    self.end_time['進入值篩選'] = time.time()
                    self._update_time_display("進入值篩選介面取消", self.end_time['進入值篩選'] - self.start_time['進入值篩選'])
                    return
                try:
                    priority = int(priority_str)
                    if priority <= 0:
                        messagebox.showwarning("輸入錯誤", f"欄位 '{col_name}' 的優先順序必須是正整數。")
                        self.end_time['進入值篩選'] = time.time()
                        self._update_time_display("進入值篩選介面取消", self.end_time['進入值篩選'] - self.start_time['進入值篩選'])
                        return
                    selected_for_filter_with_priorities.append((col_name, priority))
                except ValueError:
                    messagebox.showwarning("輸入錯誤", f"欄位 '{col_name}' 的優先順序必須是數字。")
                    self.end_time['進入值篩選'] = time.time()
                    self._update_time_display("進入值篩選介面取消", self.end_time['進入值篩選'] - self.start_time['進入值篩選'])
                    return
        
        # 如果沒有選擇任何需要篩選的欄位 (即 selected_for_filter_with_priorities 為空)
        # 但有選定要匯出的欄位 (即 self.final_export_columns 不為空)
        # 則直接進入匯出步驟
        if not selected_for_filter_with_priorities and self.final_export_columns:
            messagebox.showinfo("提示", "您沒有選擇任何需要篩選的欄位，將直接進入匯出介面並匯出所有勾選的欄位。")

            # 初始化 current_filtered_df_or_ddf 為原始數據
            if self.df is not None:
                self.current_filtered_df_or_ddf = self.df.copy()
            elif self.ddf is not None:
                self.current_filtered_df_or_ddf = self.ddf.copy()
            else:
                messagebox.showerror("錯誤", "沒有可供匯出的數據。")
                self.end_time['進入值篩選'] = time.time()
                self._update_time_display("進入值篩選介面取消", self.end_time['進入值篩選'] - self.start_time['進入值篩選'])
                return
            
            # 如果沒有篩選，直接進入匯出，並只保留 final_export_columns 中的欄位
            # 確保 self.current_filtered_df_or_ddf 確實包含這些欄位
            missing_cols = [col for col in self.final_export_columns if col not in self.current_filtered_df_or_ddf.columns]
            if missing_cols:
                messagebox.showwarning("警告", f"某些選定匯出的欄位不存在於數據中，將不會被匯出：{', '.join(missing_cols)}")
                self.final_export_columns = [col for col in self.final_export_columns if col not in missing_cols]
            
            if self.final_export_columns: # 只有當有可匯出的欄位時才進行選擇
                if isinstance(self.current_filtered_df_or_ddf, dd.DataFrame):
                    self.current_filtered_df_or_ddf = self.current_filtered_df_or_ddf[self.final_export_columns]
                elif isinstance(self.current_filtered_df_or_ddf, pd.DataFrame):
                    self.current_filtered_df_or_ddf = self.current_filtered_df_or_ddf[self.final_export_columns]
            else:
                messagebox.showwarning("警告", "沒有選定任何要匯出的欄位，將無法匯出。")
                self.end_time['進入值篩選'] = time.time()
                self._update_time_display("進入值篩選介面取消", self.end_time['進入值篩選'] - self.start_time['進入值篩選'])
                return

            self._go_to_export()
            self.end_time['進入值篩選'] = time.time()
            self._update_time_display("未選擇篩選欄位，直接進入匯出", self.end_time['進入值篩選'] - self.start_time['進入值篩選'])
            return
        
        # 如果既沒有選擇篩選欄位，也沒有選擇匯出欄位
        elif not selected_for_filter_with_priorities and not self.final_export_columns:
            messagebox.showinfo("提示", "您沒有選擇任何需要篩選或匯出的欄位。請選擇欄位。")
            self.end_time['進入值篩選'] = time.time()
            self._update_time_display("進入值篩選介面取消", self.end_time['進入值篩選'] - self.start_time['進入值篩選'])
            return

        # 根據優先順序排序欄位，數字越小優先順序越高
        self.selected_columns_prioritized = [col for col, prio in sorted(selected_for_filter_with_priorities, key=lambda x: x[1])]

        # 如果這是第一次進入篩選介面，或者要重新開始篩選，則將原始數據設為當前篩選數據
        if not self.applied_filters_display_data:
            if self.df is not None:
                self.current_filtered_df_or_ddf = self.df.copy()
            elif self.ddf is not None:
                self.current_filtered_df_or_ddf = self.ddf.copy()
            else:
                messagebox.showerror("錯誤", "沒有可篩選的數據。")
                self.end_time['進入值篩選'] = time.time()
                self._update_time_display("進入值篩選介面取消", self.end_time['進入值篩選'] - self.start_time['進入值篩選'])
                return


        if self.current_content_frame:
            self.current_content_frame.destroy()
        self.current_content_frame = ttk.Frame(self.root.winfo_children()[0], style='TFrame')
        self.current_content_frame.pack(fill=tk.BOTH, expand=True)

        self._create_value_filter_widgets(self.current_content_frame)
        self.end_time['進入值篩選'] = time.time()
        self._update_time_display("進入值篩選介面完成", self.end_time['進入值篩選'] - self.start_time['進入值篩選'])


    def _create_value_filter_widgets(self, parent_frame):
        for widget in parent_frame.winfo_children():
            widget.destroy()

        filter_main_frame = ttk.Frame(parent_frame, padding="20 20 20 20")
        filter_main_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(filter_main_frame, text="✨ 步驟 3: 選擇欄位值進行篩選",
                  font=('微軟正黑體', 12, 'bold'), background='#e0f2f7').pack(pady=10)

        # 上方框架：已篩選欄位顯示區 和 選擇要篩選的欄位
        top_filter_area_frame = ttk.Frame(filter_main_frame, style='TFrame')
        top_filter_area_frame.pack(fill=tk.X, pady=10)

        # 已篩選欄位顯示區
        applied_filters_frame = ttk.LabelFrame(top_filter_area_frame, text="✅ 已篩選欄位及保留值", padding="10")
        applied_filters_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

        self.applied_filters_text = tk.Text(applied_filters_frame, height=8, width=40, font=('微軟正黑體', 9), wrap=tk.WORD, state='disabled')
        self.applied_filters_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self._update_applied_filters_display() # 更新顯示

        # 選擇要篩選的欄位
        select_column_frame = ttk.LabelFrame(top_filter_area_frame, text="🎯 選擇要篩選的欄位", padding="10")
        select_column_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(10, 0))

        self.progress_bar = ttk.Progressbar(
            select_column_frame,
            orient="horizontal",
            length=200,
            mode="indeterminate"
        )
        self.loading_label_values = ttk.Label(select_column_frame, text="", font=('微軟正黑體', 10, 'italic'), foreground='gray')
        self.loading_label_values.pack(pady=5)

        self.filter_column_var = tk.StringVar()
        
        # 根據 selected_columns_prioritized 設置 combobox 的值
        # 篩選掉已經在 applied_filters_display_data 中的欄位 (排除 'SKIP_FILTER' 標記的欄位)
        remaining_columns_for_combobox = [
            col for col in self.selected_columns_prioritized 
            if col not in self.applied_filters_display_data or self.applied_filters_display_data[col] == 'SKIP_FILTER'
        ]

        self.filter_column_combobox = ttk.Combobox(
            select_column_frame,
            textvariable=self.filter_column_var,
            values=remaining_columns_for_combobox, # 使用排序後的欄位
            state="readonly",
            font=('微軟正黑體', 10)
        )
        if remaining_columns_for_combobox:
            self.filter_column_combobox.set("請選擇一個欄位...")
        else:
            self.filter_column_combobox.set("所有選定欄位已篩選")
            self.filter_column_combobox.config(state=tk.DISABLED)

        self.filter_column_combobox.pack(fill=tk.X, padx=5, pady=5)
        self.filter_column_var.trace_add("write", self._on_filter_column_selected)


        # 中間框架：選擇要保留的值
        value_selection_frame = ttk.LabelFrame(filter_main_frame, text="選擇要保留的值", padding="10")
        value_selection_frame.pack(fill=tk.BOTH, expand=True, pady=10)

        control_buttons_frame = ttk.Frame(value_selection_frame, style='TFrame')
        control_buttons_frame.pack(fill=tk.X, pady=(0, 5), padx=5)

        ttk.Button(control_buttons_frame, text="✅ 全選", command=lambda: self._select_all_values(True)).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_buttons_frame, text="❌ 全不選", command=lambda: self._select_all_values(False)).pack(side=tk.LEFT, padx=5)

        self.search_entry_var.set("")
        self.search_entry_var.trace_add("write", self._filter_values_by_keyword)
        self.search_entry = ttk.Entry(control_buttons_frame, textvariable=self.search_entry_var, width=30, font=('微軟正黑體', 10))
        self.search_entry.pack(side=tk.RIGHT, padx=5, fill=tk.X, expand=True)
        ttk.Label(control_buttons_frame, text="🔍 搜尋:", background='#e0f2f7').pack(side=tk.RIGHT)

        self.value_canvas = tk.Canvas(value_selection_frame, bg='#f0fbfd', highlightthickness=0)
        self.value_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.value_scrollbar = ttk.Scrollbar(value_selection_frame, orient="vertical", command=self.value_canvas.yview)
        self.value_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.value_canvas.configure(yscrollcommand=self.value_scrollbar.set)
        self.value_canvas.bind('<Configure>', lambda e: self.value_canvas.configure(scrollregion = self.value_canvas.bbox("all")))
        self.value_canvas.bind_all("<MouseWheel>", self._on_mouse_wheel_value_canvas) # For Windows/macOS
        self.value_canvas.bind_all("<Button-4>", self._on_mouse_wheel_value_canvas) # For Linux
        self.value_canvas.bind_all("<Button-5>", self._on_mouse_wheel_value_canvas) # For Linux

        self.value_checkbox_frame = ttk.Frame(self.value_canvas, style='TFrame')
        self.value_canvas.create_window((0, 0), window=self.value_checkbox_frame, anchor="nw")

        self.value_checkboxes = {} # 儲存 {value: tk.BooleanVar}

        # 下方框架：篩選操作按鈕
        filter_action_buttons_frame = ttk.Frame(filter_main_frame, style='TFrame')
        filter_action_buttons_frame.pack(pady=15)

        self.apply_filter_button = ttk.Button(filter_action_buttons_frame, text="✅ 套用篩選並進入下一步", command=self._apply_filter, state=tk.DISABLED)
        self.apply_filter_button.pack(side=tk.LEFT, padx=10)

        # 新增「不篩選，直接進入下一步」按鈕
        self.skip_filter_button = ttk.Button(filter_action_buttons_frame, text="⏩ 不篩選，直接進入下一步", command=self._skip_filter_and_go_next, state=tk.DISABLED)
        self.skip_filter_button.pack(side=tk.LEFT, padx=10)

        self.cancel_filter_button = ttk.Button(filter_action_buttons_frame, text="✖️ 取消篩選", command=self._cancel_filter)
        self.cancel_filter_button.pack(side=tk.LEFT, padx=10)

        self.process_message_label = ttk.Label(filter_main_frame, text="", font=('微軟正黑體', 10, 'italic'), foreground='gray', background='#e0f2f7')
        self.process_message_label.pack(pady=5)

        # 如果所有選定的欄位都已篩選完畢，則啟用進入匯出介面的按鈕
        if not remaining_columns_for_combobox:
            self.apply_filter_button.config(text="⬇️ 進入匯出介面", command=self._go_to_export, state=tk.NORMAL)
            self.skip_filter_button.config(state=tk.DISABLED) # 如果所有都篩選完畢，這個按鈕就沒有意義了


    def _on_mouse_wheel_value_canvas(self, event):
        if event.delta: # Windows/macOS
            self.value_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        else: # Linux
            if event.num == 4:
                self.value_canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                self.value_canvas.yview_scroll(1, "units")

    def _update_applied_filters_display(self):
        # 顯示已套用的篩選
        if hasattr(self, 'applied_filters_text') and self.applied_filters_text.winfo_exists():
            self.applied_filters_text.config(state='normal')
            self.applied_filters_text.delete(1.0, tk.END)
            if not self.applied_filters_display_data:
                self.applied_filters_text.insert(tk.END, "尚無已套用的篩選。")
            else:
                for col, values in self.applied_filters_display_data.items():
                    self.applied_filters_text.insert(tk.END, f"欄位: {col}\n")
                    if values == 'SKIP_FILTER':
                        self.applied_filters_text.insert(tk.END, "  --> 未篩選此欄位 (跳過)\n\n")
                    else:
                        display_values = []
                        for val in values:
                            display_val = "空值 (NaN/None)" if pd.isna(val) else str(val)
                            display_values.append(display_val)
                        if len(display_values) > 10:
                            self.applied_filters_text.insert(tk.END, f" 保留 {len(display_values)} 個值 (例如: {', '.join(display_values[:5])}...)\n\n")
                        else:
                            self.applied_filters_text.insert(tk.END, f" 保留值: {', '.join(display_values)}\n\n")
            self.applied_filters_text.config(state='disabled')


    def _on_filter_column_selected(self, *args):
        selected_column = self.filter_column_var.get()
        # 只有當選中的欄位不在已篩選列表中（或被標記為 SKIP_FILTER）才處理
        if selected_column and selected_column != "請選擇一個欄位..." and self.applied_filters_display_data.get(selected_column) != 'SKIP_FILTER':
            self.start_time['加載唯一值'] = time.time()
            self._update_time_display(f"加載欄位 '{selected_column}' 的唯一值")
            
            # 啟用/禁用按鈕
            if hasattr(self, 'apply_filter_button') and self.apply_filter_button.winfo_exists():
                self.apply_filter_button.config(state=tk.DISABLED, text="正在加載值，請稍候...")
            if hasattr(self, 'skip_filter_button') and self.skip_filter_button.winfo_exists():
                self.skip_filter_button.config(state=tk.DISABLED) # 載入唯一值時禁用跳過按鈕
            if hasattr(self, 'filter_column_combobox') and self.filter_column_combobox.winfo_exists():
                self.filter_column_combobox.config(state="disabled")
            
            # Disable search entry when loading values
            if hasattr(self, 'search_entry'):
                self.search_entry.config(state=tk.DISABLED) 
            if hasattr(self, 'search_entry_var'):
                self.search_entry_var.set("") # Clear search text
            
            # Clear existing widgets and data for the new column
            if hasattr(self, 'value_checkbox_frame') and self.value_checkbox_frame.winfo_exists():
                for widget in self.value_checkbox_frame.winfo_children():
                    widget.destroy()
            self.value_checkboxes.clear()

            if hasattr(self, 'loading_label_values') and self.loading_label_values.winfo_exists():
                self.loading_label_values.config(text="")
            self._start_loading_animation_general()
            if hasattr(self, 'progress_bar') and self.progress_bar.winfo_exists():
                self.progress_bar.pack(pady=5)
                self.progress_bar.start()

            thread = threading.Thread(target=self._load_unique_values_in_thread, args=(selected_column, self.current_filtered_df_or_ddf))
            thread.start()
        else: # 如果選中的欄位已經被篩選過，或者沒有選中有效欄位，則禁用套用篩選按鈕
            if hasattr(self, 'apply_filter_button') and self.apply_filter_button.winfo_exists():
                self.apply_filter_button.config(state=tk.DISABLED, text="✅ 套用篩選並進入下一步")
            if hasattr(self, 'skip_filter_button') and self.skip_filter_button.winfo_exists():
                self.skip_filter_button.config(state=tk.NORMAL) # 啟用跳過按鈕，因為沒有在載入唯一值
            if hasattr(self, 'filter_column_combobox') and self.filter_column_combobox.winfo_exists():
                self.filter_column_combobox.config(state="readonly")
            # Ensure search entry is enabled if no loading is in progress
            if hasattr(self, 'search_entry'):
                self.search_entry.config(state=tk.NORMAL)


    def _start_loading_animation_general(self):
        if hasattr(self, 'loading_label_values') and self.loading_label_values.winfo_exists():
            self.loading_label_values.config(text=self.loading_messages[self.current_message_index])
            self.current_message_index = (self.current_message_index + 1) % len(self.loading_messages)
            self.loading_animation_id_general = self.root.after(1500, self._start_loading_animation_general)


    def _stop_loading_animation_general(self):
        if self.loading_animation_id_general:
            self.root.after_cancel(self.loading_animation_id_general)
            self.loading_animation_id_general = None
        if hasattr(self, 'loading_label_values') and self.loading_label_values.winfo_exists():
            self.loading_label_values.config(text="")


    def _load_unique_values_in_thread(self, column_name, df_or_ddf):
        try:
            if isinstance(df_or_ddf, dd.DataFrame):
                # 對於 Dask DataFrame，使用 compute() 來獲取唯一值
                unique_values = df_or_ddf[column_name].unique().compute().tolist()
            else:
                # pandas.Series.unique() 回傳 numpy array，直接用 list() 包起來即可
                unique_values = list(df_or_ddf[column_name].unique())
            # 在主線程中更新 UI
            self.root.after(0, self._load_unique_values_in_thread_callback, unique_values)
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("錯誤", f"加載唯一值時發生錯誤: {e}\n您也可以考慮點擊「不篩選，直接進入下一步」跳過此欄位篩選。"))
            self.root.after(0, self._load_unique_values_in_thread_callback, []) # 傳遞空列表以停止加載動畫


    def _load_unique_values_in_thread_callback(self, unique_values):
        self._display_unique_values(unique_values)


    def _display_unique_values(self, unique_values):
        self._stop_loading_animation_general()
        if hasattr(self, 'progress_bar') and self.progress_bar.winfo_exists():
            self.progress_bar.stop()
            self.progress_bar.pack_forget()

        # Clear existing widgets before repopulating
        if hasattr(self, 'value_checkbox_frame') and self.value_checkbox_frame.winfo_exists():
            for widget in self.value_checkbox_frame.winfo_children():
                widget.destroy()
        self.value_checkboxes.clear() # Clear the dictionary as well

        # 修正 NaN 排序問題：將 NaN 和 None 排序到末尾，並將所有值轉換為字串進行比較
        self.all_unique_values_display = sorted(unique_values, key=lambda x: (pd.isna(x) or x is None, str(x)))

        row_num = 0
        for val in self.all_unique_values_display:
            display_val = "空值 (NaN/None)" if pd.isna(val) else str(val)
            
            var = tk.BooleanVar(value=True) # 預設全選
            self.value_checkboxes[val] = var # Store the original value, not the display value

            cb = ttk.Checkbutton(self.value_checkbox_frame, text=display_val, variable=var, style='TCheckbutton')
            cb.grid(row=row_num, column=0, sticky="w", padx=5, pady=2)
            row_num += 1

        self.root.update_idletasks()
        self.value_canvas.config(scrollregion=self.value_canvas.bbox("all"))

        # Re-enable apply filter button and search entry, and skip filter button
        if hasattr(self, 'apply_filter_button') and self.apply_filter_button.winfo_exists():
            self.apply_filter_button.config(state=tk.NORMAL, text="✅ 套用篩選並進入下一步")
        if hasattr(self, 'skip_filter_button') and self.skip_filter_button.winfo_exists():
            self.skip_filter_button.config(state=tk.NORMAL)
        if hasattr(self, 'search_entry'):
            self.search_entry.config(state=tk.NORMAL)
        
        # Apply any existing search keyword after values are loaded and search entry is enabled
        self._filter_values_by_keyword()

        self.end_time['加載唯一值'] = time.time()
        self._update_time_display(f"加載唯一值完成", self.end_time['加載唯一值'] - self.start_time['加載唯一值'])


    def _filter_values_by_keyword(self, *args):
        keyword = self.search_entry_var.get().lower()

        # Ensure the frame exists and is valid before trying to access its children
        if not hasattr(self, 'value_checkbox_frame') or not self.value_checkbox_frame.winfo_exists():
            return

        # Clear existing widgets
        for widget in self.value_checkbox_frame.winfo_children():
            widget.destroy()

        row_num = 0
        # Iterate through all unique values that were loaded
        for original_value in self.all_unique_values_display:
            display_val = "空值 (NaN/None)" if pd.isna(original_value) else str(original_value)
            
            # Check if the value exists in the dictionary before accessing
            if original_value in self.value_checkboxes:
                var = self.value_checkboxes[original_value]
                
                if keyword in display_val.lower():
                    cb = ttk.Checkbutton(self.value_checkbox_frame, text=display_val, variable=var, style='TCheckbutton')
                    cb.grid(row=row_num, column=0, sticky="w", padx=5, pady=2)
                    row_num += 1

        self.root.update_idletasks()
        self.value_canvas.config(scrollregion=self.value_canvas.bbox("all"))


    def _select_all_values(self, select):
        keyword = self.search_entry_var.get().lower()
        for val in self.all_unique_values_display:
            display_val = "空值 (NaN/None)" if pd.isna(val) else str(val)
            if keyword in display_val.lower():
                if val in self.value_checkboxes: # 確保鍵存在
                    self.value_checkboxes[val].set(select)

    def _apply_filter(self):
        self.start_time['套用篩選'] = time.time()
        self._update_time_display("正在套用篩選...")

        selected_column = self.filter_column_var.get()
        if not selected_column or selected_column == "請選擇一個欄位...":
            messagebox.showwarning("錯誤", "請先選擇一個要篩選的欄位。")
            self.end_time['套用篩選'] = time.time()
            self._update_time_display("套用篩選取消", self.end_time['套用篩選'] - self.start_time['套用篩選'])
            return

        selected_values = [val for val, var in self.value_checkboxes.items() if var.get()]

        # 如果選中的欄位之前被標記為 SKIP_FILTER，現在要進行實際篩選，則將其從 applied_filters_display_data 中移除
        if selected_column in self.applied_filters_display_data and self.applied_filters_display_data[selected_column] == 'SKIP_FILTER':
            del self.applied_filters_display_data[selected_column]

        if not selected_values:
            # 如果沒有選擇任何值，則彈出提示，並將其視為「跳過篩選」
            messagebox.showwarning("警告", "您沒有選擇任何值，此欄位將不進行篩選。")
            self.applied_filters_display_data[selected_column] = 'SKIP_FILTER' # 標記為跳過
            self._update_applied_filters_display()
            self._prepare_next_filter_step()
            self.end_time['套用篩選'] = time.time()
            self._update_time_display("套用篩選完成", self.end_time['套用篩選'] - self.start_time['套用篩選'])
            return

        # 處理空值 (NaN/None) 的情況
        contains_na = any(pd.isna(v) for v in self.all_unique_values_display)
        selected_na = any(pd.isna(v) for v in selected_values)

        try:
            self._start_loading_animation_process(f"正在篩選欄位 '{selected_column}'...")
            self.root.update_idletasks() # 確保訊息顯示

            if self.df is not None:
                # Pandas 篩選
                if contains_na and not selected_na:
                    # 如果有空值但沒選空值，則篩選掉空值
                    self.current_filtered_df_or_ddf = self.current_filtered_df_or_ddf[
                        (self.current_filtered_df_or_ddf[selected_column].isin(selected_values)) | 
                        (self.current_filtered_df_or_ddf[selected_column].isna() & False) # 確保篩掉 NaN
                    ]
                elif contains_na and selected_na:
                    # 如果有空值也選了空值，則保留空值
                     self.current_filtered_df_or_ddf = self.current_filtered_df_or_ddf[
                        (self.current_filtered_df_or_ddf[selected_column].isin([v for v in selected_values if not pd.isna(v)])) | 
                        (self.current_filtered_df_or_ddf[selected_column].isna())
                    ]
                else: # 沒有空值的情況
                    self.current_filtered_df_or_ddf = self.current_filtered_df_or_ddf[self.current_filtered_df_or_ddf[selected_column].isin(selected_values)]

            elif self.ddf is not None:
                # Dask 篩選
                if contains_na and not selected_na:
                    self.current_filtered_df_or_ddf = self.current_filtered_df_or_ddf[
                        (self.current_filtered_df_or_ddf[selected_column].isin(selected_values)) | 
                        (self.current_filtered_df_or_ddf[selected_column].isna() & False)
                    ]
                elif contains_na and selected_na:
                     self.current_filtered_df_or_ddf = self.current_filtered_df_or_ddf[
                        (self.current_filtered_df_or_ddf[selected_column].isin([v for v in selected_values if not pd.isna(v)])) | 
                        (self.current_filtered_df_or_ddf[selected_column].isna())
                    ]
                else:
                    self.current_filtered_df_or_ddf = self.current_filtered_df_or_ddf[self.current_filtered_df_or_ddf[selected_column].isin(selected_values)]
            
            # 更新已套用篩選的顯示
            self.applied_filters_display_data[selected_column] = selected_values
            self._update_applied_filters_display()
            self._stop_loading_animation_process()

            messagebox.showinfo("篩選成功", f"欄位 '{selected_column}' 已成功篩選。")

            # 準備進行下一步篩選或進入匯出
            self._prepare_next_filter_step()

            self.end_time['套用篩選'] = time.time()
            self._update_time_display("套用篩選完成", self.end_time['套用篩選'] - self.start_time['套用篩選'])

        except Exception as e:
            self._stop_loading_animation_process()
            messagebox.showerror("篩選錯誤", f"篩選欄位 '{selected_column}' 時發生錯誤: {e}")
            self.end_time['套用篩選'] = time.time()
            self._update_time_display("套用篩選失敗", self.end_time['套用篩選'] - self.start_time['套用篩選'])

    def _skip_filter_and_go_next(self):
        self.start_time['跳過篩選'] = time.time()
        selected_column = self.filter_column_var.get()

        if not selected_column or selected_column == "請選擇一個欄位..." or selected_column in self.applied_filters_display_data and self.applied_filters_display_data[selected_column] != 'SKIP_FILTER':
            messagebox.showwarning("警告", "請先選擇一個尚未篩選的欄位，或您已對此欄位進行過篩選。")
            self.end_time['跳過篩選'] = time.time()
            self._update_time_display("跳過篩選取消", self.end_time['跳過篩選'] - self.start_time['跳過篩選'])
            return

        # 將該欄位標記為「跳過篩選」
        self.applied_filters_display_data[selected_column] = 'SKIP_FILTER'
        self._update_applied_filters_display()
        
        messagebox.showinfo("跳過篩選", f"欄位 '{selected_column}' 已標記為不篩選，將直接進入下一步處理。")

        self._prepare_next_filter_step()
        self.end_time['跳過篩選'] = time.time()
        self._update_time_display("跳過篩選完成", self.end_time['跳過篩選'] - self.start_time['跳過篩選'])


    def _prepare_next_filter_step(self):
        # 重新整理 Combobox
        self._update_filter_column_combobox()
        self.filter_column_var.set("請選擇一個欄位...") # 重設 Combobox 顯示

        # 清空值選擇區域的內容
        if hasattr(self, 'value_checkbox_frame') and self.value_checkbox_frame.winfo_exists():
            for widget in self.value_checkbox_frame.winfo_children():
                widget.destroy()
        self.value_checkboxes.clear()
        self.all_unique_values_display = [] # 清空唯一值列表

        # 重置搜尋框
        if hasattr(self, 'search_entry_var'):
            self.search_entry_var.set("")
        if hasattr(self, 'search_entry'):
            self.search_entry.config(state=tk.DISABLED) # 預設為禁用，直到選擇下一個欄位

        # 檢查是否還有未篩選的欄位
        remaining_columns_for_combobox = [col for col in self.selected_columns_prioritized if col not in self.applied_filters_display_data or self.applied_filters_display_data[col] == 'SKIP_FILTER']
        if not remaining_columns_for_combobox:
            messagebox.showinfo("提示", "所有選定欄位都已篩選完畢，將進入匯出介面。")
            self.apply_filter_button.config(text="⬇️ 進入匯出介面", command=self._go_to_export, state=tk.NORMAL)
            self.skip_filter_button.config(state=tk.DISABLED)
            self.filter_column_combobox.config(state=tk.DISABLED) # 禁用 Combobox
        else:
            self.apply_filter_button.config(state=tk.DISABLED, text="✅ 套用篩選並進入下一步") # 禁用篩選按鈕直到選擇下一個欄位
            self.skip_filter_button.config(state=tk.NORMAL) # 啟用跳過按鈕
            self.filter_column_combobox.config(state="readonly")


    def _update_filter_column_combobox(self):
        # 排除已經被實際篩選的欄位，但包含被標記為 SKIP_FILTER 的欄位 (因為用戶可能想重新篩選它)
        remaining_columns_for_combobox = [
            col for col in self.selected_columns_prioritized 
            if col not in self.applied_filters_display_data or self.applied_filters_display_data[col] == 'SKIP_FILTER'
        ]
        self.filter_column_combobox['values'] = remaining_columns_for_combobox
        if not remaining_columns_for_combobox:
            self.filter_column_combobox.set("所有選定欄位已篩選")
            self.filter_column_combobox.config(state=tk.DISABLED)
        else:
            self.filter_column_combobox.config(state="readonly")


    def _cancel_filter(self):
        # 詢問使用者是否確定取消篩選
        if not messagebox.askyesno("取消篩選", "確定要取消所有已套用的篩選並重新開始嗎？"):
            return

        self.start_time['取消篩選'] = time.time()
        self._update_time_display("正在取消篩選並重置...")

        # 清空已套用的篩選資訊
        self.applied_filters_display_data.clear()
        self._update_applied_filters_display()

        # 重置 current_filtered_df_or_ddf 為原始數據
        if self.df is not None:
            self.current_filtered_df_or_ddf = self.df.copy()
        elif self.ddf is not None:
            self.current_filtered_df_or_ddf = self.ddf.copy()
        else:
            messagebox.showerror("錯誤", "沒有可供重置的數據。")
            self.end_time['取消篩選'] = time.time()
            self._update_time_display("取消篩選失敗", self.end_time['取消篩選'] - self.start_time['取消篩選'])
            return

        # 重新回到欄位值篩選介面，會重新加載第一個篩選欄位的唯一值
        self._create_value_filter_widgets(self.current_content_frame)

        # 重置 Combobox 和按鈕狀態
        self.filter_column_combobox.config(state="readonly")
        if self.selected_columns_prioritized:
            # 找到第一個尚未實際篩選的欄位來顯示，如果所有都被篩選或跳過了，則顯示 "所有選定欄位已篩選"
            first_unfiltered_column = next((col for col in self.selected_columns_prioritized if col not in self.applied_filters_display_data or self.applied_filters_display_data[col] == 'SKIP_FILTER'), None)
            if first_unfiltered_column:
                self.filter_column_combobox.set("請選擇一個欄位...") # 重設 Combobox 顯示
            else:
                self.filter_column_combobox.set("所有選定欄位已篩選")
                self.filter_column_combobox.config(state=tk.DISABLED)
                self.apply_filter_button.config(text="⬇️ 進入匯出介面", command=self._go_to_export, state=tk.NORMAL)
                self.skip_filter_button.config(state=tk.DISABLED)
                
        else:
            self.filter_column_combobox.set("沒有可篩選的欄位")
            self.filter_column_combobox.config(state=tk.DISABLED)

        self.apply_filter_button.config(text="✅ 套用篩選並進入下一步", command=self._apply_filter, state=tk.DISABLED)
        self.skip_filter_button.config(state=tk.DISABLED) # 預設禁用，直到選擇欄位

        self.end_time['取消篩選'] = time.time()
        self._update_time_display("取消篩選完成", self.end_time['取消篩選'] - self.start_time['取消篩選'])


    def _start_loading_animation_process(self, message):
        if hasattr(self, 'process_message_label') and self.process_message_label.winfo_exists():
            self.process_message_label.config(text=message)
            # 這裡可以添加一個簡單的動畫，例如每隔一段時間更新文字
            # 為了簡潔，暫時只顯示文字
            pass # 這裡可以添加動畫邏輯


    def _stop_loading_animation_process(self):
        if hasattr(self, 'process_message_label') and self.process_message_label.winfo_exists():
            self.process_message_label.config(text="")


    def _go_to_export(self):
        self.start_time['進入匯出'] = time.time()
        self._update_time_display("進入匯出介面")

        if self.current_filtered_df_or_ddf is None:
            messagebox.showwarning("警告", "沒有數據可以匯出。請先選擇一個檔案並進行篩選。")
            self.end_time['進入匯出'] = time.time()
            self._update_time_display("進入匯出介面取消", self.end_time['進入匯出'] - self.start_time['進入匯出'])
            return
        
        # 檢查是否有任何欄位被選定為最終匯出欄位
        if not self.final_export_columns:
            messagebox.showwarning("警告", "您沒有選擇任何需要匯出的欄位。請返回步驟2選擇欄位。")
            self.end_time['進入匯出'] = time.time()
            self._update_time_display("進入匯出介面取消", self.end_time['進入匯出'] - self.start_time['進入匯出'])
            return


        if self.current_content_frame:
            self.current_content_frame.destroy()
        self.current_content_frame = ttk.Frame(self.root.winfo_children()[0], style='TFrame')
        self.current_content_frame.pack(fill=tk.BOTH, expand=True)

        self._create_export_widgets(self.current_content_frame)

        self.end_time['進入匯出'] = time.time()
        self._update_time_display("進入匯出介面完成", self.end_time['進入匯出'] - self.start_time['進入匯出'])


    def _create_export_widgets(self, parent_frame):
        for widget in parent_frame.winfo_children():
            widget.destroy()

        export_main_frame = ttk.Frame(parent_frame, padding="20 20 20 20")
        export_main_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(export_main_frame, text="⬇️ 步驟 4: 匯出結果",
                  font=('微軟正黑體', 12, 'bold'), background='#e0f2f7').pack(pady=10)

        # 顯示已套用的篩選
        applied_filters_export_frame = ttk.LabelFrame(export_main_frame, text="✅ 已套用的篩選", padding="10")
        applied_filters_export_frame.pack(fill=tk.X, pady=10)
        self.applied_filters_text_export = tk.Text(applied_filters_export_frame, height=5, font=('微軟正黑體', 9), wrap=tk.WORD, state='disabled')
        self.applied_filters_text_export.pack(fill=tk.X, padx=5, pady=5)
        self._update_applied_filters_display_export()

        # 匯出選項
        export_options_frame = ttk.LabelFrame(export_main_frame, text="匯出設定", padding="10")
        export_options_frame.pack(fill=tk.X, pady=10)

        self.export_format_var = tk.StringVar(value="csv")
        ttk.Label(export_options_frame, text="匯出格式:", background='#e0f2f7').pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Radiobutton(export_options_frame, text="CSV", variable=self.export_format_var, value="csv", style='TCheckbutton').pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(export_options_frame, text="Excel", variable=self.export_format_var, value="xlsx", style='TCheckbutton').pack(side=tk.LEFT, padx=5)

        ttk.Label(export_options_frame, text="自定義檔名 (選填):", background='#e0f2f7').pack(side=tk.LEFT, padx=(20, 5), pady=5)
        self.custom_filename_var = tk.StringVar()
        ttk.Entry(export_options_frame, textvariable=self.custom_filename_var, width=30, style='TEntry').pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        export_button = ttk.Button(export_main_frame, text="💾 匯出檔案", command=self._export_file)
        export_button.pack(pady=15)

        back_button = ttk.Button(export_main_frame, text="↩️ 返回篩選", command=self._go_back_to_filter)
        back_button.pack(pady=5)


    def _update_applied_filters_display_export(self):
        if hasattr(self, 'applied_filters_text_export') and self.applied_filters_text_export.winfo_exists():
            self.applied_filters_text_export.config(state='normal')
            self.applied_filters_text_export.delete(1.0, tk.END)
            if not self.applied_filters_display_data:
                self.applied_filters_text_export.insert(tk.END, "尚無已套用的篩選。")
            else:
                for col, values in self.applied_filters_display_data.items():
                    self.applied_filters_text_export.insert(tk.END, f"欄位: {col}\n")
                    if values == 'SKIP_FILTER':
                        self.applied_filters_text_export.insert(tk.END, "  --> 未篩選此欄位 (跳過)\n\n")
                    else:
                        display_values = []
                        for val in values:
                            display_val = "空值 (NaN/None)" if pd.isna(val) else str(val)
                            display_values.append(display_val)
                        if len(display_values) > 10:
                            self.applied_filters_text_export.insert(tk.END, f" 保留 {len(display_values)} 個值 (例如: {', '.join(display_values[:5])}...)\n\n")
                        else:
                            self.applied_filters_text_export.insert(tk.END, f" 保留值: {', '.join(display_values)}\n\n")
            self.applied_filters_text_export.config(state='disabled')


    def _export_file(self):
        self.start_time['檔案匯出'] = time.time()
        self._update_time_display("正在匯出檔案...")
        
        if self.current_filtered_df_or_ddf is None:
            messagebox.showwarning("警告", "沒有數據可以匯出。")
            self.end_time['檔案匯出'] = time.time()
            self._update_time_display("檔案匯出取消", self.end_time['檔案匯出'] - self.start_time['檔案匯出'])
            return
        
        # 再次檢查是否有任何欄位被選定為最終匯出欄位
        if not self.final_export_columns:
            messagebox.showwarning("警告", "您沒有選擇任何需要匯出的欄位。請返回步驟2選擇欄位。")
            self.end_time['檔案匯出'] = time.time()
            self._update_time_display("檔案匯出取消", self.end_time['檔案匯出'] - self.start_time['檔案匯出'])
            return

        export_format = self.export_format_var.get()
        custom_filename = self.custom_filename_var.get().strip()

        default_filename = "filtered_data"
        if self.file_path:
            # 從原始檔案名稱派生預設檔名
            base_filename = os.path.splitext(os.path.basename(self.file_path))[0]
            default_filename = f"{base_filename}_filtered"

        filename = custom_filename if custom_filename else default_filename
        
        # 詢問使用者儲存路徑
        file_path = filedialog.asksaveasfilename(
            defaultextension=f".{export_format}",
            filetypes=[(f"{export_format.upper()} files", f"*.{export_format}")],
            initialfile=filename
        )

        if not file_path:
            self.end_time['檔案匯出'] = time.time()
            self._update_time_display("檔案匯出取消", self.end_time['檔案匯出'] - self.start_time['檔案匯出'])
            return

        try:
            self._start_loading_animation_process("正在處理匯出...")
            self.root.update_idletasks()

            def export_thread_target():
                try:
                    if isinstance(self.current_filtered_df_or_ddf, dd.DataFrame):
                        # Dask DataFrame 匯出需要 compute()
                        result_df = self.current_filtered_df_or_ddf.compute()
                    else:
                        result_df = self.current_filtered_df_or_ddf

                    # 檢查並只選擇 self.final_export_columns 中存在的欄位
                    existing_cols_to_export = [col for col in self.final_export_columns if col in result_df.columns]
                    if existing_cols_to_export:
                        result_df = result_df[existing_cols_to_export]
                    else:
                        # 如果沒有任何選定的欄位存在於結果數據中
                        self.root.after(0, self._stop_loading_animation_process)
                        self.root.after(0, lambda: messagebox.showerror("匯出錯誤", "沒有任何選定的欄位存在於最終處理後的數據中，無法匯出。"))
                        self.root.after(0, lambda: self._update_time_display("檔案匯出失敗", time.time() - self.start_time['檔案匯出']))
                        return # 提前返回

                    if export_format == "csv":
                        result_df.to_csv(file_path, index=False, encoding='utf-8-sig') # 確保中文不亂碼
                    elif export_format == "xlsx":
                        result_df.to_excel(file_path, index=False)
                    
                    self.root.after(0, self._stop_loading_animation_process)
                    self.root.after(0, lambda: messagebox.showinfo("匯出成功", f"檔案已成功匯出至:\n{file_path}"))
                    self.root.after(0, lambda: self._update_time_display("檔案匯出完成", time.time() - self.start_time['檔案匯出']))
                except Exception as e:
                    self.root.after(0, self._stop_loading_animation_process)
                    self.root.after(0, lambda: messagebox.showerror("匯出錯誤", f"匯出檔案時發生錯誤: {e}"))
                    self.root.after(0, lambda: self._update_time_display("檔案匯出失敗", time.time() - self.start_time['檔案匯出']))

            export_thread = threading.Thread(target=export_thread_target)
            export_thread.start()

        except Exception as e:
            self._stop_loading_animation_process()
            messagebox.showerror("匯出錯誤", f"無法啟動匯出程序: {e}")
            self.end_time['檔案匯出'] = time.time()
            self._update_time_display("檔案匯出失敗", self.end_time['檔案匯出'] - self.start_time['檔案匯出'])


    def _go_back_to_filter(self):
        self.start_time['返回篩選'] = time.time()
        self._update_time_display("返回篩選介面")

        if hasattr(self, 'search_entry_var'):
            self.search_entry_var.set("")
        if hasattr(self, 'column_search_entry_var'):
            self.column_search_entry_var.set("")

        if hasattr(self, 'current_content_frame') and self.current_content_frame.winfo_exists():
            for widget in self.current_content_frame.winfo_children():
                widget.destroy()

        main_frame_children = self.root.winfo_children()
        if main_frame_children and main_frame_children[0].winfo_exists(): # 確保 main_frame 存在
            # 重新實例化 current_content_frame，因為它可能在 _go_to_value_filter 中被銷毀了
            # 確保是在 root.winfo_children()[0] (也就是 main_frame) 內部創建
            self.current_content_frame = ttk.Frame(main_frame_children[0], style='TFrame')
            self.current_content_frame.pack(fill=tk.BOTH, expand=True)
            self._create_column_selection_widgets(self.current_content_frame)

        # 修正：只有在沒有選擇檔案時才顯示「未選擇檔案`
        if not self.file_path:
            self.file_label.config(text="未選擇檔案")
        # 否則保留原本的檔案名稱顯示

        # 清空所有已套用的篩選資訊
        self.applied_filters_display_data.clear()
        
        # 重置 current_filtered_df_or_ddf 為 None，讓它可以重新從原始數據開始篩選
        self.current_filtered_df_or_ddf = None
        self.df = None
        self.ddf = None
        self.file_path = None # 清除檔案路徑，讓使用者重新選擇檔案
        self.final_export_columns = [] # 新增：清除匯出欄位列表

        # 重置所有欄位選擇和篩選狀態
        self._clear_column_selection()
        # self._display_columns(self.all_columns_display) # 這裡不需要，因為沒有文件打開，all_columns_display是空的

        self.next_step_button.config(state=tk.DISABLED) # 確保返回後，下一步按鈕是禁用的

        self.end_time['返回篩選'] = time.time()
        self._update_time_display("返回篩選介面完成", self.end_time['返回篩選'] - self.start_time['返回篩選'])

if __name__ == "__main__":
    import tkinter as tk
    root = tk.Tk()
    app = DataAnalyzerApp(root)
    root.mainloop()