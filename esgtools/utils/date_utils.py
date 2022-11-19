
from datetime import date
from pandas.tseries.offsets import BDay
from pytz import timezone


def get_last_business_date(asof):
    """Returns last business date that has closed in NY
        
        Parameters
        ----------
        asof: datetime.datetime or datetime.date

        Returns
        -------
        datetime.date
            Last business date that has closed in NY.
            If self.today is a weekend or holiday, it returns the last business date.
            If self.today is a business date, then it returns the same date if it is past
            4 pm ET, otherwise the last business date.
    """

    if asof:
        # Check whether it is a business day
        is_business_day = (asof - BDay(1) + BDay(1)).date() == asof.date()

        # Check whether stock market is closed in USA
        time_eastern = asof.astimezone(timezone('US/Eastern'))
        is_after_4_pm = time_eastern.hour >= 16

        # If weekday and after 4 pm ET
        if is_business_day and is_after_4_pm:
            return asof.date()

        # If weekend, holiday or before 4 pm then return last business day
        return (asof.date() - BDay(1)).date()
    return None


def get_last_quarter_date(asof):
    if asof:
        if asof.month < 4:
            return date(asof.year - 1, 12, 31)
        elif asof.month < 7:
            return date(asof.year, 3, 31)
        elif asof.month < 10:
            return date(asof.year, 6, 30)
        return date(asof.year, 9, 30)
    return None