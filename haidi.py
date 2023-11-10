import efinance as ef
import arrow
import os
import argparse
import pandas as pd
import traceback



class Argparse:
    def __init__(self):
        self.stock = Stock()
        self.strategy = Strategy()
        self.parser = argparse.ArgumentParser()
        self.subparsers = self.parser.add_subparsers(dest="subcomm")

        parser_a = self.subparsers.add_parser('query', help='query stock msg')
        parser_a.add_argument("-n", dest="name", help="stock name")
        parser_a.add_argument("-d", "--date", dest="date", nargs="+")
        parser_a.add_argument("-t", "--tail", dest="tail", default=5, type=int)
        parser_a.add_argument("-bk", "--sector", dest="sector", action="store_true")
        parser_a.add_argument("-rt", "--realtime", dest="realtime", action="store_true")
        parser_a.add_argument("-c", "--choice", dest="choice", action="store_true")

        parser_b = self.subparsers.add_parser('download', help='download stock data')
        parser_b.add_argument("-t", dest="type", type=str, choices=['30', '60', '00', 'longhu'], help="download data type: 30 60 00 68、longhu list")
        parser_b.add_argument("-d", "--date", dest="date", nargs="+")

        parser_c = self.subparsers.add_parser('analyze', help='analyze stock data')
        parser_c.add_argument("-d", "--date", dest="date", nargs="+")

        parser_d = self.subparsers.add_parser('operate', help='operate stock')
        parser_d.add_argument("-b", "--buy",  dest="buy_price", type=float)
        parser_d.add_argument("-s", "--sell", dest="sell_price", type=float)

        parser_e = self.subparsers.add_parser('add', help='add stock code to file')
        parser_e.add_argument("-n", dest="name", help="stock name", required=True)
        parser_e.add_argument("-d", "--date", dest="date", required=True)


        args = self.parser.parse_args()

        # python haidi.py query -n 300502 -d 20231009 20231111 -bk
        if args.subcomm == "query":
            if args.realtime:
                print(self.stock.query(args.name, type="realtime").head(args.tail + 15))
            elif args.sector:
                print(self.stock.query(args.name, type="sector").head(args.tail + 5))
            else:
                beg=args.date[0]
                end=args.date[0] if len(args.date) == 1 else args.date[1]
                print(self.stock.query(args.name,beg,end).tail(args.tail))
        elif args.subcomm == "download":
            beg = args.date[0]
            end = args.date[0] if len(args.date) == 1 else args.date[1]
            self.stock.download(args.type, beg, end)
        elif args.subcomm == "analyze":
            self.strategy.haidi(args.date[0])
        elif args.subcomm == "operate":
            self.stock.operate()
        elif args.subcomm == "add":
            self.stock.add(args.name,args.date)


class Stock:
    def __init__(self):
        HERE = os.path.dirname(__file__)
        self.longhu_file = os.path.join(HERE,"longhu.csv")
        self.choice_file = os.path.join(HERE,"choice.csv")

    def get_stock_dates_v2(self,date,beg_shift=0,end_shift=0,format=""):
        beg = '20080101'
        end = '22770101'
        dates = ef.stock.get_quote_history('600519', beg=beg, end=end)['日期']
        if format == "":
            date = date.replace('-', '')
            dates = dates.str.replace('-', '')
        else:
            date = arrow.get(date).format("YYYY-MM-DD")
        date_index = dates[dates.str.contains(date)].index[0]
        # + 1 是因为列表截取是左闭右开
        date_range = dates.values[date_index - beg_shift: date_index + end_shift + 1]
        return date_range

    def buy(self, code_name, price):
        print(code_name, price)

    def sell(self, code_name, price):
        print(code_name, price)

    def query(self, code_name, beg=None, end=None, type=None):
        if type == "realtime":
            return ef.stock.get_realtime_quotes('创业板')
        elif type == "sector":
            return ef.stock.get_belong_board(code_name)
        else:
            return ef.stock.get_quote_history(code_name,beg,end)

    def download(self, type, beg, end):
        if type == "longhu":
            beg = arrow.get(beg).format("YYYY-MM-DD")
            end = arrow.get(end).format("YYYY-MM-DD")
            billboard = ef.stock.get_daily_billboard(start_date=beg, end_date=end)
            billboard.to_csv(self.longhu_file, encoding="gbk", index=False)

    def operate(self):
        pass

    def add(self, code_name, date):
        qoute = ef.stock.get_quote_history(code_name,beg=date,end=date)[["股票代码","股票名称","日期","收盘"]]
        qoute.to_csv(self.choice_file,encoding="gbk", index=False, mode="a", header=False)


class Strategy:
    def __init__(self):
        self.stock = Stock()
        self.today = arrow.now().format('YYYYMMDD')

    def chg_format(self, num1, num2):
        res = 0 if num2 == 0 or num1 == 0 else round(((num2 - num1) / num1) * 100, 2)
        return res

    def get_chg_1_3_5_10_day(self, stock_codes, date):
        stock_codes = stock_codes if type(stock_codes) == list else [stock_codes]
        date_range = self.stock.get_stock_dates_v2(date,beg_shift=7,end_shift=11,format="-")
        beg = date_range[0].replace("-","")
        end = date_range[-1].replace("-","")
        beg2 = self.stock.get_stock_dates_v2(date,beg_shift=30)[0]
        end2 = date
        res = pd.DataFrame(
            columns=['stock_name', 'stock_code','beg', 'colse', 'cap', 'volume_chg', 'chg_-5', 'chg_-3', 'chg_-1', 'chg', 'ch1', 'chg_1', 'chg_3',
                     'chg_5', 'chg_10','bk_chg_-3'])
        # last_df,用最新的数据查当时的市值
        df = ef.stock.get_quote_history(stock_codes, beg=beg, end=end)
        df2 = ef.stock.get_quote_history(stock_codes, beg=beg2, end=end2)
        last_df = ef.stock.get_quote_history(stock_codes, beg=self.today)
        for stock_code in df.keys():
            try:
                quote = df[stock_code]
                if quote.empty:
                    continue
                quote2 = df2[stock_code]
                # quote2["rsi"] = talib.RSI(quote2['收盘'])
                quote2["rsi"] = None
                # 最新数据
                quote = pd.merge(quote, pd.Series(date_range, name='日期'), how='right',on='日期').fillna(0)
                if quote.shape[0] != 17:
                    quote = quote.reindex([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17], fill_value=0)
                close_df = quote['收盘']
                # 由于时间根据当前日期前推7天后推10天，则当前天的索引则是7
                beg_index = 7
                close_beg = close_df[beg_index]
                open_beg =  quote['开盘'].iloc[beg_index + 1]
                volumes = quote['成交量'].values
                volume_chg = volumes[beg_index] / volumes[beg_index - 5: beg_index].mean()
                # 需要减去指定日期的前一天，所以是2 4 6，而非1 3 5
                chg_lst1 = [self.chg_format(close_df[beg_index - i], close_df[beg_index - 1]) for i in [2, 4, 6]]
                _chg_1, _chg_3, _chg_5 = chg_lst1
                chg_lst2 = [self.chg_format(open_beg, close_df[beg_index + i]) for i in [1, 3, 5]]
                chg_1, chg_3, chg_5 = chg_lst2
                chg_10 = self.chg_format(open_beg, close_df[close_df != 0].iloc[-1])
                ch1 = quote['涨跌幅'][beg_index + 1]

                stock_name = quote['股票名称'].iloc[beg_index]
                chg = quote['涨跌幅'][beg_index]
                close = quote['收盘'][beg_index]
                cap = round((ef.stock.get_base_info(stock_code)['总市值'] / 100000000), 2)
                last_close = last_df[stock_code]['收盘'][0]
                real_cap = round((close_beg / last_close) * cap, 2)
                _bk_chg_3 = quote2.iloc[-1]['rsi']
                res.loc[len(res)] = [stock_name, stock_code, date.replace('-', ''), close, real_cap, volume_chg, _chg_5, _chg_3, _chg_1,
                                     chg, ch1, chg_1, chg_3, chg_5, chg_10,_bk_chg_3]
            except Exception as e:
                print(f"------------{date},{stock_code}")
                traceback.print_exc()
        return res

    def haidi(self, date):
        billboard = pd.read_csv(self.stock.longhu_file, encoding='gbk', dtype=object)
        billboard_30 = billboard.query(
            "股票代码.str.startswith('30') & ~股票名称.str.contains('退') & ~股票名称.str.contains('ST') & 涨跌幅>='0'")
        stock_codes = billboard_30['股票代码'].drop_duplicates().to_list()
        stock_codes = stock_codes if type(stock_codes) == list else [stock_codes]
        dfs = {}
        for i in stock_codes:
            bank = ef.stock.get_belong_board(i)
            bank = bank.query("~板块名称.str.contains('板块') & ~板块名称.str.contains('创业板') "
                              "& ~板块名称.str.contains('昨日') & ~板块名称.str.contains('次新股') "
                              "& ~板块名称.str.contains('深股通') & ~板块名称.str.contains('高送转') "
                              "& ~板块名称.str.contains('沪股通') & ~板块名称.str.contains('ST股') "
                              "& ~板块名称.str.contains('融资融券') & ~板块名称.str.contains('重仓')")
            dfs[bank['股票名称'].iloc[0]] = bank

        banks = pd.concat(dfs, axis=0, ignore_index=True)

        banks.sort_values(by='板块名称', inplace=True, ascending=False)
        name_counts = banks['板块名称'].value_counts()
        less_than_3 = name_counts[name_counts < 3].index.tolist()
        filtered_df = banks[~banks['板块名称'].isin(less_than_3)]

        name_counts_2 = filtered_df['股票名称'].value_counts()
        less_than_3_2 = name_counts_2[name_counts_2 >= 2].index.tolist()

        print(name_counts[name_counts != 1])
        print(filtered_df)
        print(less_than_3_2)

        res = self.get_chg_1_3_5_10_day(stock_codes, date.replace('-', ''))
        print(res)
        print(res.query('chg_1 >= 19 or chg_3 >= 19 or chg_5 >= 19 or chg_10 >= 19').reset_index(drop=True))


if __name__ == '__main__':
    ag = Argparse()
