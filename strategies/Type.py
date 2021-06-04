import datetime
import enum


class Type(enum.Enum):
    PERCENT = {"type": "percent", "prefix": "", "suffix": "%"}
    AMOUNT = {"type": "amount", "prefix": "$", "suffix": ""}
    AMOUNT_MILLIONS = {"type": "amount", "prefix": "$", "suffix": "M"}
    AMOUNT_BILLIONS = {"type": "amount", "prefix": "$", "suffix": "B"}
    COUNT = {"type": "count", "prefix": "", "suffix": ""}
    COUNT_MILLIONS = {"type": "count", "prefix": "", "suffix": "M"}
    COUNT_BILLIONS = {"type": "count", "prefix": "", "suffix": "B"}
    STRING = {"type": "string", "prefix": "", "suffix": ""}
    URL = {"type": "url", "prefix": "", "suffix": ""}
    NUMBER = {"type": "number", "prefix": "", "suffix": ""}
    DATE = {"type": "date", "prefix": "", "suffix": ""}
    DAYS = {"type": "days", "prefix": "", "suffix": ""}

    @staticmethod
    def cast(_type, value):
        try:
            if _type in [
                Type.PERCENT,
                Type.AMOUNT_BILLIONS,
                Type.AMOUNT_MILLIONS,
                Type.AMOUNT,
                Type.COUNT_BILLIONS,
                Type.COUNT_MILLIONS,
                Type.COUNT,
                Type.NUMBER,
                Type.DAYS,
            ]:
                return float(value)
            elif _type == Type.DATE:
                return datetime.datetime.combine(
                    datetime.date.fromisoformat(value), datetime.datetime.min.time()
                )
            else:
                return value
        except ValueError:
            return value
