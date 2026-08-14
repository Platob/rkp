import rkp
import rkp.records


def test_records_exports_are_available_from_the_package_and_root() -> None:
    for name in rkp.records.__all__:
        assert getattr(rkp.records, name) is getattr(rkp, name)
