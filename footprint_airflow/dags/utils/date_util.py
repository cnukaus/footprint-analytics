from datetime import datetime, timedelta

import pandas as pd
import pytz


class DateUtil(object):
    IND_TIMEZONE = pytz.timezone('Asia/Kolkata')
    ZH_TIMEZONE = pytz.timezone('Asia/Shanghai')
    UTC_TIMEZONE = pytz.timezone('UTC')

    @classmethod
    def ind_current(cls) -> datetime:
        return datetime.now(cls.IND_TIMEZONE)

    @classmethod
    def zh_current(cls) -> datetime:
        return datetime.now(cls.ZH_TIMEZONE)

    @classmethod
    def utc_current(cls) -> datetime:
        return datetime.now(cls.UTC_TIMEZONE)

    @classmethod
    def utc_start_of_date(cls, dt: datetime = None) -> datetime:
        if not dt:
            dt = cls.utc_current()
        return cls.UTC_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day))

    @classmethod
    def get_date(cls) -> datetime:
        return datetime.now(cls.UTC_TIMEZONE) - timedelta(days=1)

    @classmethod
    def utc_24h_ago(cls) -> datetime:
        return datetime.now(cls.UTC_TIMEZONE) - timedelta(days=1)

    @classmethod
    def utc_x_hours_ago(cls, x: int, dt: datetime = None) -> datetime:
        if not dt:
            dt = cls.utc_current()
        return dt - timedelta(hours=x)

    @classmethod
    def utc_x_hours_after(cls, x: int, dt: datetime = None) -> datetime:
        if not dt:
            dt = cls.utc_current()
        return dt + timedelta(hours=x)

    @classmethod
    def utc_to_ind(cls, dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = cls.UTC_TIMEZONE.localize(dt)
        return dt.astimezone(cls.IND_TIMEZONE)

    @classmethod
    def ind_to_utc(cls, dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = cls.IND_TIMEZONE.localize(dt)
        return dt.astimezone(cls.UTC_TIMEZONE)

    @classmethod
    def zh_to_ind(cls, dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = cls.ZH_TIMEZONE.localize(dt)
        return dt.astimezone(cls.IND_TIMEZONE)

    @classmethod
    def zh_to_utc(cls, dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = cls.ZH_TIMEZONE.localize(dt)
        return dt.astimezone(cls.UTC_TIMEZONE)

    @classmethod
    def ind_of_date(cls, dt: datetime):
        return cls.IND_TIMEZONE.localize(dt)

    @classmethod
    def ind_start_of_date(cls, dt: datetime = None):
        if not dt:
            dt = cls.ind_current()
        return cls.IND_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day))

    @classmethod
    def ind_end_of_date(cls, dt: datetime = None):
        if not dt:
            dt = cls.ind_current()
        return cls.IND_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day, 23, 59, 59, 999))

    @classmethod
    def ind_end_of_day_seconds(cls, dt: datetime = None):
        if not dt:
            dt = cls.ind_current()
        diff = cls.ind_end_of_date() - dt
        second = diff.seconds + 1 if diff.microseconds > 0 else diff.seconds
        return second

    @classmethod
    def zh_start_of_date(cls, dt: datetime = None):
        if not dt:
            dt = cls.zh_current()
        return cls.ZH_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day))

    @classmethod
    def zh_end_of_date(cls, dt: datetime = None):
        if not dt:
            dt = cls.zh_current()
        return cls.ZH_TIMEZONE.localize(datetime(dt.year, dt.month, dt.day, 23, 59, 59, 999))

    @classmethod
    def zh_end_of_day_seconds(cls, dt: datetime = None):
        if not dt:
            dt = cls.zh_current()
        diff = cls.zh_end_of_date() - dt
        second = diff.seconds + 1 if diff.microseconds > 0 else diff.seconds
        return second

    @classmethod
    def days_diff(cls, start: datetime, end: datetime):
        st = cls.ind_start_of_date(cls.utc_to_ind(start))
        et = cls.ind_start_of_date(cls.utc_to_ind(end))
        return (et - st).days

    @classmethod
    def tz_localize(cls, dt_sr: pd.Series, tz: str) -> pd.Series:
        return dt_sr.dt.tz_localize(tz)
