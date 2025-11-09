class C2CException(Exception):
    pass


class C2CExceptionBadTimeframeValue(C2CException):
    def __init__(self, value):
        self.value = value
        super().__init__(f'Bad timeframe value: {value}')
