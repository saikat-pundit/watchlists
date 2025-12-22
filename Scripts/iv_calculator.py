import datetime
import scipy.stats
import numpy as np
from scipy.stats import norm
from enum import Enum, IntEnum
from scipy.optimize import brentq
from datetime import datetime as dt, timedelta
from numpy import abs as ABS, exp as EXP, log as LOG, sqrt as SQRT
from typing import Tuple, List, Dict, Literal, Union, Any

NORM_CDF = norm.cdf
NORM_PDF = norm.pdf

# Indian Holidays 2025&2026 (Updated)
HOLIDAYS = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-25", "2026-01-26", "2026-03-03", 
    "2026-03-26", "2026-03-31", "2026-04-03", "2026-04-14", 
    "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14", 
    "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24", 
    "2026-12-25"
]
CURRENTYEAR = str(dt.now().year)
NEXTYEAR = str(dt.now().year + 1)


class ExpType(Enum):
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


class DayCountType(IntEnum):
    CALENDARDAYS = 365
    BUSINESSDAYS = np.busday_count(
        begindates=str(CURRENTYEAR),
        enddates=str(NEXTYEAR),
        weekmask="1111100",
    )
    TRADINGDAYS = np.busday_count(
        begindates=str(CURRENTYEAR),
        enddates=str(NEXTYEAR),
        weekmask="1111100",
        holidays=HOLIDAYS,
    )


class TryMatchWith(Enum):
    NSE = "NSE"
    CUSTOM = "CUSTOM"
    SENSIBULL = "SENSIBULL"


class FromDateType(IntEnum):
    FIXED = 0
    DYNAMIC = 1


class CalcIvGreeks:
    """Main class for calculating Implied Volatility and Greeks"""
    
    TD64S = "timedelta64[s]"
    IV_LOWER_BOUND = 1e-11
    SECONDS_IN_A_DAY = np.timedelta64(1, "D").astype(TD64S)

    def __init__(
        self,
        SpotPrice: float,
        FuturePrice: float,
        AtmStrike: float,
        AtmStrikeCallPrice: float,
        AtmStrikePutPrice: float,
        ExpiryDateTime: dt,
        StrikePrice: Union[float, None] = None,
        StrikeCallPrice: Union[float, None] = None,
        StrikePutPrice: Union[float, None] = None,
        ExpiryDateType: ExpType = ExpType.MONTHLY,
        FromDateTime: Union[dt, None] = None,
        tryMatchWith: TryMatchWith = TryMatchWith.SENSIBULL,
        dayCountType: DayCountType = DayCountType.CALENDARDAYS,
    ) -> None:
        self.dateFuture = ExpiryDateTime
        self.datePast = dt.now() if FromDateTime is None else FromDateTime
        self.datePastType = (
            FromDateType.FIXED
            if self.datePast.microsecond == FromDateType.FIXED
            else FromDateType.DYNAMIC
        )
        self.dayCountType = dayCountType
        self.tryMatchWith = tryMatchWith
        self.F = (
            FuturePrice
            if ExpiryDateType == ExpType.MONTHLY
            else AtmStrikeCallPrice - AtmStrikePutPrice + AtmStrike
        )
        self.K0 = AtmStrike
        self.C0 = AtmStrikeCallPrice
        self.P0 = AtmStrikePutPrice
        self.S = SpotPrice if self.tryMatchWith == TryMatchWith.NSE else self.F
        if StrikePrice is not None:
            self.K = StrikePrice
        if StrikeCallPrice is not None:
            self.C = StrikeCallPrice
        if StrikePutPrice is not None:
            self.P = StrikePutPrice
        self.r = (
            0.1
            if self.tryMatchWith == TryMatchWith.NSE
            else 0.0
            if self.tryMatchWith == TryMatchWith.SENSIBULL
            else self.getRiskFreeIntrRate() / 100
        )
        self.T = self.get_tte()

    def update(
        self,
        SpotPrice: float,
        FuturePrice: float,
        AtmStrike: float,
        AtmStrikeCallPrice: float,
        AtmStrikePutPrice: float,
        FromDateTime: Union[dt, None] = None,
    ) -> None:
        if FromDateTime is not None:
            self.datePast = FromDateTime
            self.datePastType = FromDateType.FIXED
        self.S = SpotPrice if self.tryMatchWith == TryMatchWith.NSE else self.F
        self.K0 = AtmStrike
        self.C0 = AtmStrikeCallPrice
        self.P0 = AtmStrikePutPrice
        self.F = (
            FuturePrice
            if hasattr(self, 'expiryDateType') and self.expiryDateType == ExpType.MONTHLY
            else self.C0 - self.P0 + self.K0
        )
        self.T = self.get_tte()

    @staticmethod
    def getRiskFreeIntrRate() -> float:
        import pandas as pd
        import requests
        
        try:
            return (
                pd.json_normalize(
                    requests.get(
                        "https://techfanetechnologies.github.io"
                        + "/risk_free_interest_rate/RiskFreeInterestRate.json"
                    ).json()
                )
                .query('GovernmentSecurityName == "364 day T-bills"')
                .reset_index()
                .Percent[0]
            )
        except:
            return 6.0  # Default 6% if fetch fails

    @staticmethod
    def find_atm_strike(all_strikes: List[float], ltp: float) -> float:
        return float(min(all_strikes, key=lambda x: abs(x - ltp)))

    def refreshNow(self) -> None:
        if self.datePastType == FromDateType.DYNAMIC:
            self.datePast = dt.now()

    def get_dte(self) -> float:
        if self.dayCountType == DayCountType.CALENDARDAYS:
            return (
                np.datetime64(
                    dt.combine(
                        self.dateFuture.date(), datetime.time(15, 30, 0)
                    )
                )
                - np.datetime64(self.datePast)
            ).astype(self.TD64S) / self.SECONDS_IN_A_DAY
        else:
            return (
                (
                    np.busday_count(
                        begindates=self.datePast.date(),
                        enddates=(self.dateFuture + timedelta(days=1)).date(),
                        weekmask="1111100",
                        holidays=HOLIDAYS,
                    )
                    * self.SECONDS_IN_A_DAY
                )
                - (
                    np.datetime64(
                        int(
                            timedelta(
                                hours=8, minutes=30, seconds=0
                            ).total_seconds()
                        ),
                        "s",
                    )
                ).astype(self.TD64S)
                - (
                    np.datetime64(self.datePast)
                    - np.datetime64(
                        dt.combine(
                            self.datePast.date(), datetime.time(0, 0, 0)
                        )
                    )
                ).astype(self.TD64S)
            ) / self.SECONDS_IN_A_DAY

    def get_tte(self) -> float:
        self.refreshNow()
        return float(
            self.get_dte()
            / (
                self.dayCountType.value
                if (
                    (
                        self.dayCountType == DayCountType.BUSINESSDAYS
                        and self.datePast.year == self.dateFuture.year
                    )
                    or (
                        self.dayCountType == DayCountType.TRADINGDAYS
                        and self.datePast.year == self.dateFuture.year
                    )
                )
                else (
                    np.busday_count(
                        begindates=self.datePast.date(),
                        enddates=f"{self.datePast.year+1}-01-01",
                        weekmask="1111100",
                    )
                    + np.busday_count(
                        begindates=str(self.dateFuture.year),
                        enddates=str(self.dateFuture.year + 1),
                        weekmask="1111100",
                    )
                )
                if (
                    self.dayCountType == DayCountType.BUSINESSDAYS
                    and (self.dateFuture.year > self.datePast.year)
                    and (self.dateFuture.year - self.datePast.year == 1)
                )
                else (
                    np.busday_count(
                        begindates=self.datePast.date(),
                        enddates=f"{self.datePast.year+1}-01-01",
                        weekmask="1111100",
                        holidays=HOLIDAYS,
                    )
                    + np.busday_count(
                        begindates=str(self.dateFuture.year),
                        enddates=str(self.dateFuture.year + 1),
                        weekmask="1111100",
                        holidays=HOLIDAYS,
                    )
                )
                if (
                    self.dayCountType == DayCountType.TRADINGDAYS
                    and (self.dateFuture.year > self.datePast.year)
                    and (self.dateFuture.year - self.datePast.year == 1)
                )
                else np.busday_count(
                    begindates=self.datePast.date(),
                    enddates=(self.dateFuture + timedelta(days=1)).date(),
                    weekmask="1111100",
                )
                if (
                    self.dayCountType == DayCountType.BUSINESSDAYS
                    and (self.dateFuture.year > self.datePast.year)
                    and (self.dateFuture.year - self.datePast.year >= 2)
                )
                else np.busday_count(
                    begindates=self.datePast.date(),
                    enddates=(self.dateFuture + timedelta(days=1)).date(),
                    weekmask="1111100",
                    holidays=HOLIDAYS,
                )
                if (
                    self.dayCountType == DayCountType.TRADINGDAYS
                    and (self.dateFuture.year > self.datePast.year)
                    and (self.dateFuture.year - self.datePast.year >= 2)
                )
                else DayCountType.CALENDARDAYS.value
            )
        )

    def CND(self, d: float):
        A1 = 0.31938153
        A2 = -0.356563782
        A3 = 1.781477937
        A4 = -1.821255978
        A5 = 1.330274429
        RSQRT2PI = 0.39894228040143267793994605993438
        K = 1.0 / (1.0 + 0.2316419 * ABS(d))
        ret_val = (
            RSQRT2PI
            * EXP(-0.5 * d * d)
            * (K * (A1 + K * (A2 + K * (A3 + K * (A4 + K * A5)))))
        )
        return np.where(d > 0, 1.0 - ret_val, ret_val)

    def BSM(self, sigma: float):
        sqrtT = SQRT(self.T)
        d1 = (
            LOG(self.S / self.K) + (self.r + 0.5 * sigma * sigma) * self.T
        ) / (
            sigma * sqrtT
        )
        d2 = d1 - sigma * sqrtT
        cndd1, cndd2 = self.CND(d1), self.CND(d2)
        expRT = EXP(-self.r * self.T)
        return expRT, cndd1, cndd2

    def BS_CallPutPrice(self, sigma: float):
        expRT, cndd1, cndd2 = self.BSM(sigma)
        BS_CallPrice = self.S * cndd1 - self.K * expRT * cndd2
        BS_PutPrice = self.K * expRT * (1.0 - cndd2) - self.S * (1.0 - cndd1)
        return BS_CallPrice, BS_PutPrice

    def BS_CallPrice(self, sigma: float):
        expRT, cndd1, cndd2 = self.BSM(sigma)
        return self.S * cndd1 - self.K * expRT * cndd2

    def BS_PutPrice(self, sigma: float):
        expRT, cndd1, cndd2 = self.BSM(sigma)
        return self.K * expRT * (1.0 - cndd2) - self.S * (1.0 - cndd1)

    def BS_d1(self, sigma: float):
        if sigma > self.IV_LOWER_BOUND:
            return (
                LOG(self.S / self.K) + (self.r + sigma**2 / 2) * self.T
            ) / (
                sigma * SQRT(self.T)
            )
        return np.inf if self.S > self.K else -np.inf

    def BS_d2(self, sigma: float):
        return self.BS_d1(sigma) - (sigma * SQRT(self.T))

    def BS_CallPricing(self, sigma: float):
        return NORM_CDF(self.BS_d1(sigma)) * self.S - NORM_CDF(
            self.BS_d2(sigma)
        ) * self.K * EXP(-self.r * self.T)

    def BS_PutPricing(self, sigma: float):
        return (
            NORM_CDF(-self.BS_d2(sigma)) * self.K * EXP(-self.r * self.T)
            - NORM_CDF(-self.BS_d1(sigma)) * self.S
        )

    def DeltaCall(self, sigma: float):
        return NORM_CDF(self.BS_d1(sigma))

    def DeltaPut(self, sigma: float):
        return NORM_CDF(self.BS_d1(sigma)) - 1

    def Gamma(self, sigma: float) -> float:
        if sigma > self.IV_LOWER_BOUND:
            return NORM_PDF(self.BS_d1(sigma)) / (
                self.S * sigma * SQRT(self.T)
            )
        return 0

    def Vega(self, sigma: float) -> float:
        return NORM_PDF(self.BS_d1(sigma)) * self.S * SQRT(self.T)

    def ThetaCall(self, sigma: float) -> float:
        return -self.S * sigma * NORM_PDF(self.BS_d1(sigma)) / (
            2 * SQRT(self.T)
        ) - self.r * self.K * EXP(-self.r * self.T) * NORM_CDF(
            self.BS_d2(sigma)
        )

    def ThetaPut(self, sigma: float) -> float:
        return -self.S * sigma * NORM_PDF(self.BS_d1(sigma)) / (
            2 * SQRT(self.T)
        ) + self.r * self.K * EXP(-self.r * self.T) * NORM_CDF(
            -self.BS_d2(sigma)
        )

    def RhoCall(self, sigma: float) -> float:
        return (
            self.K
            * self.T
            * EXP(-self.r * self.T)
            * NORM_CDF(self.BS_d2(sigma))
        )

    def RhoPut(self, sigma: float) -> float:
        return (
            -self.K
            * self.T
            * EXP(-self.r * self.T)
            * NORM_CDF(-self.BS_d2(sigma))
        )

    def ImplVolWithBrent(self, OptionLtp, PricingFunction):
        try:
            ImplVol = brentq(
                lambda sigma: OptionLtp - PricingFunction(sigma),
                0.001,  # Lower bound
                5.0,    # Upper bound (500% IV)
                xtol=1e-12,
                maxiter=100
            )
            return (
                ImplVol
                if ImplVol > self.IV_LOWER_BOUND
                else self.IV_LOWER_BOUND
            )
        except Exception:
            return self.IV_LOWER_BOUND

    def CallImplVol(self):
        return self.ImplVolWithBrent(self.C, self.BS_CallPricing)

    def PutImplVol(self):
        return self.ImplVolWithBrent(self.P, self.BS_PutPricing)

    def GetImpVolAndGreeks(
        self,
        StrikePrice: Union[float, None] = None,
        StrikeCallPrice: Union[float, None] = None,
        StrikePutPrice: Union[float, None] = None,
    ) -> Dict:
        if StrikePrice is not None:
            self.K = StrikePrice
        if StrikeCallPrice is not None:
            self.C = StrikeCallPrice
        if StrikePutPrice is not None:
            self.P = StrikePutPrice
        self.refreshNow()
        CallIV = round(self.CallImplVol(), 6)
        PutIV = round(self.PutImplVol(), 6)
        StrikeIV = CallIV if self.K >= self.K0 else PutIV
        Delta = round(self.DeltaCall(StrikeIV), 2)
        if self.tryMatchWith == TryMatchWith.NSE:
            _ = {
                "CallIV": round(CallIV * 100, 2),
                "PutIV": round(PutIV * 100, 2),
            }
        else:
            _ = {}
        return {
            **{
                "Strike": self.K,
                "ImplVol": round(StrikeIV * 100, 2),
            },
            **_,
            **{
                "CallDelta": Delta,
                "PutDelta": round(Delta - 1, 2),
                "Theta": round((self.ThetaPut(StrikeIV) / 365), 2),
                "Vega": round((self.Vega(StrikeIV) / 100), 2),
                "Gamma": round(self.Gamma(StrikeIV), 4),
                "RhoCall": round(self.RhoCall(CallIV) / 1000, 3),
                "RhoPut": round(self.RhoPut(PutIV) / 1000, 3),
            },
        }
