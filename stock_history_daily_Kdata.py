import akshare as ak
import pandas as pd

class DataSource:
    def __init__(self):
        '''# 实时行情数据-东财- 可获取整个A股股票实时信息
            https://akshare.akfamily.xyz/data/stock/stock.html#id1）
            print(df_stock_kc_data_pd.shape[0])
        '''
        self.df_stock_all_data_pd = ak.stock_zh_a_spot_em()    # 全量实时数据, 可获取全A股实施数据（涨跌幅	涨跌额	成交量	成交额	振幅	最高	...	量比	换手率	市盈率-动态	市净率	总市值	流通市值	涨速	5分钟涨跌	60日涨跌幅	年初至今涨跌幅）
        self.df_stock_kc_data_pd = ak.stock_kc_a_spot_em()     # 科创板实时数据
        self.df_stock_ja_data_pd = ak.stock_bj_a_spot_em()     # 京A实时数据
        self.df_stock_st_data_pd = ak.stock_zh_a_st_em()       # st股，风险警示股
        #self.df_stock_cy_data_pd = ak.stock_cy_a_spot_em()    # 创业板实时数据,print(df_stock_cy_data_pd.shape[0])

        print("A股股票总数:", len(self.df_stock_all_data_pd['代码'].unique()))
        print("科创股票总数:", len(self.df_stock_kc_data_pd['代码'].unique()))
        print("京a股票总数:", len(self.df_stock_ja_data_pd['代码'].unique()))
        print("st股票总数:", len(self.df_stock_st_data_pd['代码'].unique()))
        
        self.usefull_stockcode_set = set(self.df_stock_all_data_pd['代码'].unique()) - set(self.df_stock_kc_data_pd['代码'].unique()) \
                                    - set(self.df_stock_ja_data_pd['代码'].unique()) - set(self.df_stock_st_data_pd['代码'].unique())
        
        print("剔除科创-京a-st两市后的个股数量:", len(self.usefull_stockcode_set))
        
        



def fetch_and_merge_stocks(stock_symbols, period="daily", start_date="20180101", end_date="20241205"):
    """
    获取多个股票的K线数据并合并到一个DataFrame中。

    参数:
        stock_symbols (list): 股票代码列表。
        period (str): 数据周期，可选 "daily"、"weekly" 等。
        start_date (str): 开始日期，格式为 "YYYYMMDD"。
        end_date (str): 结束日期，格式为 "YYYYMMDD"。
        adjust (str): 复权方式，可选 "hfq"（后复权）、"qfq"（前复权）或 "none"（不复权）。

    返回:
        pd.DataFrame: 合并后的股票数据。
    """
    # 创建一个空的 DataFrame，用于存储合并数据
    all_stocks_data = pd.DataFrame()

    # 遍历每个股票代码
    for symbol in stock_symbols:
        try:
            # 获取股票数据
            df_stock = ak.stock_zh_a_hist(
                symbol=symbol, 
                period=period, 
                start_date=start_date, 
                end_date=end_date
            )
            # 添加股票代码列，用于区分不同股票
            df_stock["股票代码"] = symbol
            
            # 添加前一日收盘价列
            df_stock["pre_close"] = df_stock["收盘"].shift(1)
            
            # 将当前股票的数据合并到总 DataFrame
            all_stocks_data = pd.concat([all_stocks_data, df_stock], ignore_index=True)
        except Exception as e:
            print(f"获取股票 {symbol} 数据时出错: {e}")

    # 返回合并后的数据
    return all_stocks_data

# 示例调用
# stock_list = ["000001", "000002","000004"]

dataSource = DataSource()
stock_list = dataSource.usefull_stockcode_set

merged_data = fetch_and_merge_stocks(
    stock_symbols=stock_list, 
    period="daily", 
    start_date="20241223", 
    end_date="20250103"
)

print(f"合并后的数据行数: {merged_data.shape[0]}")
merged_data.to_parquet("A_stock_history_daily_Kdata_20241223_20250103.parquet",index=False)
