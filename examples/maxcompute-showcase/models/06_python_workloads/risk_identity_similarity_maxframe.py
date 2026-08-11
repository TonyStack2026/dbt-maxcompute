import maxframe.dataframe as md
import pandas as pd
from maxframe.udf import with_python_requirements


@with_python_requirements(
    "rapidfuzz==3.14.5",
    "text-unidecode==1.3",
    prefer_binary=True,
)
def score_identity_pair(row):
    from rapidfuzz import fuzz
    from sklearn.feature_extraction.text import HashingVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    from text_unidecode import unidecode

    def normalize(value):
        return " ".join(unidecode(str(value or "")).lower().split())

    left_name = normalize(row["left_name"])
    right_name = normalize(row["right_name"])
    left_email = normalize(row["left_email"]).replace("+temp", "")
    right_email = normalize(row["right_email"]).replace("+temp", "")

    name_ratio = float(fuzz.WRatio(left_name, right_name))
    email_ratio = float(fuzz.ratio(left_email, right_email))
    vectorizer = HashingVectorizer(
        analyzer="char_wb",
        ngram_range=(2, 4),
        n_features=128,
        alternate_sign=False,
        norm="l2",
    )
    vectors = vectorizer.transform([f"{left_name} {left_email}", f"{right_name} {right_email}"])
    cosine = float(cosine_similarity(vectors[0], vectors[1])[0][0])
    phone_suffix_match = int(str(row["left_phone"])[-7:] == str(row["right_phone"])[-7:])
    composite = (
        name_ratio * 0.35 + email_ratio * 0.30 + cosine * 100 * 0.25 + phone_suffix_match * 10
    )

    return pd.Series(
        {
            "left_customer_id": str(row["left_customer_id"]),
            "right_customer_id": str(row["right_customer_id"]),
            "country_code": str(row["country_code"]),
            "name_similarity": name_ratio,
            "email_similarity": email_ratio,
            "hashed_cosine_similarity": cosine,
            "phone_suffix_match": phone_suffix_match,
            "composite_similarity": min(100.0, composite),
            "possible_duplicate": bool(composite >= 72),
        }
    )


def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="maxframe",
        lifecycle=1,
        timeout=5400,
        maxframe_retries=2,
        # The managed CP311 sklearn image provides NumPy, SciPy and
        # scikit-learn without an expensive per-job native package build.
        sql_hints={"odps.session.image": "sklearn"},
        tags=["complex_python", "maxframe_heavy_dependency"],
    )

    customers = dbt.ref("risk_customers")
    left = customers[["customer_id", "customer_name", "email", "phone", "country_code"]].rename(
        columns={
            "customer_id": "left_customer_id",
            "customer_name": "left_name",
            "email": "left_email",
            "phone": "left_phone",
        }
    )
    right = customers[["customer_id", "customer_name", "email", "phone", "country_code"]].rename(
        columns={
            "customer_id": "right_customer_id",
            "customer_name": "right_name",
            "email": "right_email",
            "phone": "right_phone",
        }
    )
    pairs = left.merge(right, on="country_code", how="inner")
    pairs = pairs[pairs["left_customer_id"] < pairs["right_customer_id"]]

    output_dtypes = pd.Series(
        {
            "left_customer_id": md.dtype("string"),
            "right_customer_id": md.dtype("string"),
            "country_code": md.dtype("string"),
            "name_similarity": md.dtype("float64"),
            "email_similarity": md.dtype("float64"),
            "hashed_cosine_similarity": md.dtype("float64"),
            "phone_suffix_match": md.dtype("int64"),
            "composite_similarity": md.dtype("float64"),
            "possible_duplicate": md.dtype("boolean"),
        }
    )
    return pairs.apply(
        score_identity_pair,
        axis=1,
        result_type="expand",
        output_type="dataframe",
        dtypes=output_dtypes,
    )
