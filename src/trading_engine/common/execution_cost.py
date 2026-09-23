"""Decimal execution cost helpers; persisted monetary values are decimal strings."""
from decimal import Decimal, InvalidOperation
from typing import Any


def decimal_value(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("Invalid execution decimal") from exc
    if not result.is_finite() or result < 0:
        raise ValueError("Execution decimal must be finite and nonnegative")
    return result


def decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def cumulative_quote(quantity: Any, average: Any, quote: Any = None) -> str | None:
    """Prefer exchange cumulative quote; zero/missing average means unknown cost."""
    qty = decimal_value(quantity) if quantity is not None else None
    if qty is None:
        return None
    if quote is not None:
        amount = decimal_value(quote)
        if amount > 0 or qty == 0:
            return str(amount)
    if average is not None:
        price = decimal_value(average)
        if price > 0:
            return str(qty * price)
    return "0" if qty == 0 else None
