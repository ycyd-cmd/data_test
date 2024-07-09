import datetime

class data_toolkit:
    @staticmethod
    def convert_date_format(date_str, output_format='%Y-%m-%d'):
        """
        将输入的日期字符串从多种可能的格式转换成指定的格式。

        参数:
            date_str (str): 输入的日期字符串，支持的格式包括:
                            'YYMMDD', 'YYYYMMDD', 'YYYY-MM-DD', 'YYYY.MM.DD'
            output_format (str): 输出日期字符串的格式，默认为 '%Y-%m-%d'

        返回:
            str: 转换后的日期字符串

        异常:
            ValueError: 如果日期格式不符合任何已知模式或者无法解析
        """
        date_formats = [
            '%y%m%d',    # YYMMDD
            '%Y%m%d',    # YYYYMMDD
            '%Y-%m-%d',  # YYYY-MM-DD
            '%Y.%m.%d'   # YYYY.MM.DD
        ]
        
        for format in date_formats:
            try:
                # 尝试按照当前格式解析日期
                parsed_date = datetime.datetime.strptime(date_str, format)
                # 如果解析成功，按照指定的输出格式返回日期字符串
                return parsed_date.strftime(output_format)
            except ValueError:
                # 如果当前格式不匹配，则继续尝试下一个格式
                continue
        
        # 如果所有格式都试过后还是失败，抛出异常
        raise ValueError("输入的日期格式不符合任何已知模式: {}".format(date_str))

# # 使用示例
# toolkit = data_toolkit()
# try:
#     print(toolkit.convert_date_format("220418", "%Y-%m-%d"))  # 将 '220418' 转换成 '2022-04-18'
#     print(toolkit.convert_date_format("20220418", "%Y/%m/%d"))  # 将 '20220418' 转换成 '2022/04/18'
#     print(toolkit.convert_date_format("2022-04-18", "%d-%m-%Y"))  # 将 '2022-04-18' 转换成 '18-04-2022'
#     print(toolkit.convert_date_format("2022.04.18", "%d.%m.%Y"))  # 将 '2022.04.18' 转换成 '18.04.2022'
# except ValueError as e:
#     print(e)
