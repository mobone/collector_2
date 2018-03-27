from pandas_datareader.yahoo.options import Options
def get_option(symbol):
    o = Options(symbol)
    forward_data = o.get_forward_data(3, call=True, put=True).reset_index()
