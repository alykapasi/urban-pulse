def test_package_is_importable() -> None:
    import urbanpulse

    assert urbanpulse.__version__ == "0.1.0"
