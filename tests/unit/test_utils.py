from unittest.mock import patch

import pytest

from dbt.adapters.maxcompute.utils import retry_on_transport_error


def test_retry_on_transport_error_recovers_from_transient_failure():
    attempts = 0

    def operation():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionResetError("transient")
        return "done"

    with patch("dbt.adapters.maxcompute.utils.time.sleep") as sleep:
        assert retry_on_transport_error(operation) == "done"

    assert attempts == 3
    assert [call.args[0] for call in sleep.call_args_list] == [0.5, 1.0]


def test_retry_on_transport_error_does_not_mask_final_failure():
    operation = lambda: (_ for _ in ()).throw(ConnectionResetError("persistent"))

    with (
        patch("dbt.adapters.maxcompute.utils.time.sleep"),
        pytest.raises(ConnectionResetError, match="persistent"),
    ):
        retry_on_transport_error(operation)


def test_retry_on_transport_error_does_not_retry_non_transport_failure():
    attempts = 0

    def operation():
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid request")

    with (
        patch("dbt.adapters.maxcompute.utils.time.sleep") as sleep,
        pytest.raises(ValueError, match="invalid request"),
    ):
        retry_on_transport_error(operation)

    assert attempts == 1
    sleep.assert_not_called()
