# ==============================================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Framework: Glue2Lakehouse v4.0 (Production Ready)
# ==============================================================================
#
# Review and test before production use
#
# Transformations:
#   - Removed Job class
#   - Converted DynamicFrame -> DataFrame
#   - Updated type annotations
#   - Syntax: PASSED
#
# ==============================================================================


"""
Loan Data Models
Core data structures for loan processing.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Dict, Any, Optional
from enum import Enum
from decimal import Decimal

class LoanStatus(Enum):
    """Loan lifecycle status."""
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    FUNDED = "FUNDED"
    ACTIVE = "ACTIVE"
    DELINQUENT = "DELINQUENT"
    DEFAULT = "DEFAULT"
    PAID_OFF = "PAID_OFF"
    CHARGED_OFF = "CHARGED_OFF"

class LoanType(Enum):
    """Type of loan product."""
    AUTO_NEW = "AUTO_NEW"
    AUTO_USED = "AUTO_USED"
    PERSONAL = "PERSONAL"
    HOME_EQUITY = "HOME_EQUITY"
    REFINANCE = "REFINANCE"

@dataclass
class Applicant:
    """Loan applicant information."""
    applicant_id: str
    first_name: str
    last_name: str
    ssn_hash: str  # Hashed SSN for matching
    date_of_birth: date
    annual_income: Decimal
    employment_status: str
    employer_name: Optional[str] = None
    years_employed: Optional[int] = None
    monthly_housing_payment: Decimal = Decimal("0")

    @property
    def age(self) -> int:
        today = date.today()
        return today.year - self.date_of_birth.year

@dataclass
class Vehicle:
    """Vehicle information for auto loans."""
    vin: str
    make: str
    model: str
    year: int
    mileage: int
    condition: str  # NEW, EXCELLENT, GOOD, FAIR, POOR
    nada_value: Decimal
    purchase_price: Decimal

    @property
    def ltv_ratio(self) -> float:
        """Loan to value ratio."""
        if self.nada_value > 0:
            return float(self.purchase_price / self.nada_value)
        return 0.0

@dataclass
class LoanApplication:
    """Complete loan application."""
    application_id: str
    applicant: Applicant
    loan_type: LoanType
    requested_amount: Decimal
    term_months: int
    purpose: str
    application_date: datetime
    status: LoanStatus = LoanStatus.PENDING
    co_applicant: Optional[Applicant] = None
    vehicle: Optional[Vehicle] = None
    dealer_id: Optional[str] = None
    down_payment: Decimal = Decimal("0")
    trade_in_value: Decimal = Decimal("0")

    @property
    def financed_amount(self) -> Decimal:
        """Amount to be financed after down payment."""
        base = self.requested_amount
        if self.vehicle:
            base = self.vehicle.purchase_price
        return base - self.down_payment - self.trade_in_value

    @property
    def dti_ratio(self) -> float:
        """Debt to income ratio estimate."""
        # Estimate monthly payment
        monthly_payment = float(self.financed_amount) / self.term_months
        total_debt = monthly_payment + float(self.applicant.monthly_housing_payment)
        monthly_income = float(self.applicant.annual_income) / 12

        if monthly_income > 0:
            return total_debt / monthly_income
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "application_id": self.application_id,
            "applicant_id": self.applicant.applicant_id,
            "loan_type": self.loan_type.value,
            "requested_amount": float(self.requested_amount),
            "term_months": self.term_months,
            "purpose": self.purpose,
            "application_date": self.application_date.isoformat(),
            "status": self.status.value,
            "financed_amount": float(self.financed_amount),
            "dti_ratio": self.dti_ratio
        }

@dataclass
class PaymentSchedule:
    """Amortization schedule for a loan."""
    loan_id: str
    payment_number: int
    due_date: date
    principal_amount: Decimal
    interest_amount: Decimal
    total_payment: Decimal
    remaining_balance: Decimal
    paid: bool = False
    paid_date: Optional[date] = None
    paid_amount: Optional[Decimal] = None

    @property
    def is_late(self) -> bool:
        """Check if payment is late."""
        if self.paid:
            return False
        return date.today() > self.due_date

    @property
    def days_past_due(self) -> int:
        """Days past due."""
        if self.paid or date.today() <= self.due_date:
            return 0
        return (date.today() - self.due_date).days

@dataclass
class LoanAccount:
    """Active loan account."""
    loan_id: str
    application_id: str
    applicant_id: str
    loan_type: LoanType
    original_amount: Decimal
    current_balance: Decimal
    interest_rate: Decimal
    term_months: int
    origination_date: date
    maturity_date: date
    status: LoanStatus
    payments_made: int = 0
    payments_remaining: int = 0
    days_delinquent: int = 0
    payment_schedule: List[PaymentSchedule] = field(default_factory=list)

    @property
    def monthly_payment(self) -> Decimal:
        """Calculate monthly payment."""
        r = float(self.interest_rate) / 12 / 100
        n = self.term_months
        p = float(self.original_amount)

        if r == 0:
            return Decimal(str(p / n))

        payment = p * (r * (1 + r) ** n) / ((1 + r) ** n - 1)
        return Decimal(str(round(payment, 2)))

    @property
    def payoff_amount(self) -> Decimal:
        """Current payoff amount including accrued interest."""
        daily_rate = float(self.interest_rate) / 365 / 100
        accrued = float(self.current_balance) * daily_rate * 10  # 10-day estimate
        return self.current_balance + Decimal(str(round(accrued, 2)))

def calculate_amortization(principal: Decimal, rate: Decimal,
                           term_months: int, start_date: date) -> List[PaymentSchedule]:
    """
    Calculate full amortization schedule.

    Args:
        principal: Loan principal
        rate: Annual interest rate (e.g., 5.99)
        term_months: Loan term in months
        start_date: First payment date

    Returns:
        List of PaymentSchedule entries
    """
    schedule = []
    balance = float(principal)
    monthly_rate = float(rate) / 12 / 100

    # Calculate monthly payment
    if monthly_rate == 0:
        monthly_payment = balance / term_months
    else:
        monthly_payment = balance * (monthly_rate * (1 + monthly_rate) ** term_months) / \
                         ((1 + monthly_rate) ** term_months - 1)

    current_date = start_date

    for i in range(1, term_months + 1):
        interest = balance * monthly_rate
        principal_paid = monthly_payment - interest
        balance -= principal_paid

        if balance < 0:
            balance = 0

        schedule.append(PaymentSchedule(
            loan_id="",  # Set by caller
            payment_number=i,
            due_date=current_date,
            principal_amount=Decimal(str(round(principal_paid, 2))),
            interest_amount=Decimal(str(round(interest, 2))),
            total_payment=Decimal(str(round(monthly_payment, 2))),
            remaining_balance=Decimal(str(round(balance, 2)))
        ))

        # Next month
        if current_date.month == 12:
            current_date = date(current_date.year + 1, 1, current_date.day)
        else:
            try:
                current_date = date(current_date.year, current_date.month + 1, current_date.day)
            except ValueError:
                # Handle month-end edge cases
                current_date = date(current_date.year, current_date.month + 1, 28)

    return schedule
