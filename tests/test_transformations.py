import hw_transformations


def test_parse_damage_amount(spark):
    """Test NCEI magnitude-suffixed damage string parsing (e.g. '10.00K', '1.5M')."""
    data = [(1, "10.00K"), (2, "1.5M"), (3, "2B"), (4, "0.00K"), (5, None), (6, "")]
    df = spark.createDataFrame(data, ["id", "damage_property_raw"])

    df = hw_transformations.parse_damage_amount(df, "damage_property_raw", "damage_property_usd")
    result = {row["id"]: row["damage_property_usd"] for row in df.collect()}

    assert result[1] == 10_000.0
    assert result[2] == 1_500_000.0
    assert result[3] == 2_000_000_000.0
    assert result[4] == 0.0
    assert result[5] == 0.0
    assert result[6] == 0.0


def test_severity_score(spark):
    """Test composite severity score weighting deaths > injuries > damage."""
    data = [(1, 1, 0, 0.0), (2, 0, 1, 0.0), (3, 0, 0, 1_000_000.0), (4, None, None, None)]
    df = spark.createDataFrame(data, ["id", "deaths_direct", "injuries_direct", "damage_total_usd"])

    df = hw_transformations.severity_score(df, "deaths_direct", "injuries_direct", "damage_total_usd", "severity_score")
    result = {row["id"]: row["severity_score"] for row in df.collect()}

    assert result[1] == 100.0
    assert result[2] == 10.0
    assert result[3] == 1.0
    assert result[4] == 0.0
    assert result[1] > result[2] > result[3]


def test_derive_state_abbr_from_ugc(spark):
    """Test 2-letter state abbreviation extraction from a UGC zone code."""
    data = [(1, "DCZ001"), (2, "vaz028"), (3, None)]
    df = spark.createDataFrame(data, ["id", "ugc"])

    df = df.select("id", hw_transformations.derive_state_abbr_from_ugc("ugc", "state_abbr"))
    result = {row["id"]: row["state_abbr"] for row in df.collect()}

    assert result[1] == "DC"
    assert result[2] == "VA"
    assert result[3] is None
