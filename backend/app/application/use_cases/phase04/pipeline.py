from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import normalize

try:
    from annoy import AnnoyIndex
except Exception:  # pragma: no cover - optional dependency
    AnnoyIndex = None


@dataclass
class ContentArtifacts:
    movie_ids: List[int]
    vectors: np.ndarray


CANDIDATE_COLUMNS = [
    "userId",
    "movieId",
    "score_pop",
    "score_item",
    "score_user",
    "score_content",
    "score_final",
]


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _parse_timestamp(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit="s", errors="coerce")
    return pd.to_datetime(series, errors="coerce")


def load_movies(path: str, max_rows: Optional[int] = None) -> pd.DataFrame:
    return pd.read_csv(
        path,
        nrows=max_rows,
        usecols=["movieId", "title", "genres"],
        dtype={"movieId": "int32", "title": "string", "genres": "string"},
    )


def load_ratings(path: str, max_rows: Optional[int] = None) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        nrows=max_rows,
        usecols=["userId", "movieId", "rating", "timestamp"],
        dtype={"userId": "int32", "movieId": "int32", "rating": "float32"},
    )
    df["timestamp"] = _parse_timestamp(df["timestamp"])
    return df.dropna(subset=["timestamp"])


def load_tags(path: str, max_rows: Optional[int] = None) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        nrows=max_rows,
        usecols=["userId", "movieId", "tag", "timestamp"],
        dtype={"userId": "int32", "movieId": "int32", "tag": "string"},
    )
    df["timestamp"] = _parse_timestamp(df["timestamp"])
    df["tag"] = df["tag"].fillna("")
    return df.dropna(subset=["timestamp"])


def split_train_test_by_time(
    ratings_df: pd.DataFrame, holdout: int = 1
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = ratings_df.copy()
    df["timestamp"] = _parse_timestamp(df["timestamp"])
    df = df.dropna(subset=["timestamp"])

    train_parts = []
    test_parts = []
    for _, group in df.groupby("userId"):
        group = group.sort_values("timestamp")
        if len(group) <= holdout:
            train_parts.append(group)
            continue
        train_parts.append(group.iloc[:-holdout])
        test_parts.append(group.iloc[-holdout:])

    train_df = pd.concat(train_parts, ignore_index=True) if train_parts else df.iloc[:0]
    test_df = pd.concat(test_parts, ignore_index=True) if test_parts else df.iloc[:0]
    return train_df, test_df


def build_popularity(ratings_df: pd.DataFrame, decay: float = 0.01) -> pd.DataFrame:
    df = ratings_df.copy()
    df["timestamp"] = _parse_timestamp(df["timestamp"])
    df = df.dropna(subset=["timestamp"])
    max_ts = df["timestamp"].max()

    grouped = df.groupby("movieId")
    rating_count = grouped.size()
    rating_mean = grouped["rating"].mean()
    last_ts = grouped["timestamp"].max()
    age_days = (max_ts - last_ts).dt.total_seconds() / 86400.0
    score = np.log1p(rating_count) * np.exp(-decay * age_days)

    popularity = pd.DataFrame(
        {
            "movieId": rating_count.index.astype("int32"),
            "rating_count": rating_count.values.astype("int32"),
            "rating_mean": rating_mean.values.astype("float32"),
            "score_popularity": score.values.astype("float32"),
        }
    )
    return popularity.sort_values("score_popularity", ascending=False)


def _build_interaction_matrix(
    ratings_df: pd.DataFrame, min_rating: Optional[float]
) -> Tuple[np.ndarray, np.ndarray, sparse.csr_matrix]:
    df = ratings_df[["userId", "movieId", "rating"]].copy()
    if min_rating is not None:
        df = df[df["rating"] >= min_rating]
    df = df.groupby(["userId", "movieId"], as_index=False)["rating"].mean()

    user_ids = np.sort(df["userId"].unique())
    movie_ids = np.sort(df["movieId"].unique())
    user_index = {user_id: idx for idx, user_id in enumerate(user_ids)}
    movie_index = {movie_id: idx for idx, movie_id in enumerate(movie_ids)}

    rows = df["movieId"].map(movie_index).to_numpy()
    cols = df["userId"].map(user_index).to_numpy()
    data = df["rating"].to_numpy(dtype="float32")

    matrix = sparse.coo_matrix(
        (data, (rows, cols)), shape=(len(movie_ids), len(user_ids))
    ).tocsr()
    return movie_ids, user_ids, matrix


def _build_neighbors(
    matrix: sparse.csr_matrix, ids: np.ndarray, top_k: int
) -> Dict[int, List[Tuple[int, float]]]:
    if matrix.shape[0] == 0:
        return {}

    n_neighbors = min(top_k + 1, matrix.shape[0])
    knn = NearestNeighbors(metric="cosine", algorithm="brute", n_neighbors=n_neighbors)
    knn.fit(matrix)
    distances, indices = knn.kneighbors(matrix, return_distance=True)

    neighbors: Dict[int, List[Tuple[int, float]]] = {}
    for row_idx, source_id in enumerate(ids):
        row_neighbors: List[Tuple[int, float]] = []
        for nbr_idx, dist in zip(indices[row_idx], distances[row_idx]):
            if nbr_idx == row_idx:
                continue
            score = max(0.0, 1.0 - float(dist))
            row_neighbors.append((int(ids[nbr_idx]), score))
        neighbors[int(source_id)] = row_neighbors[:top_k]
    return neighbors


def build_interaction_neighbors(
    ratings_df: pd.DataFrame,
    top_k_item: int,
    top_k_user: int,
    min_rating: Optional[float],
) -> Tuple[Dict[int, List[Tuple[int, float]]], Dict[int, List[Tuple[int, float]]]]:
    movie_ids, user_ids, item_user = _build_interaction_matrix(
        ratings_df, min_rating=min_rating
    )
    item_neighbors = _build_neighbors(item_user, movie_ids, top_k_item)
    user_neighbors = _build_neighbors(item_user.T.tocsr(), user_ids, top_k_user)
    return item_neighbors, user_neighbors


def build_content_embeddings(
    movies_df: pd.DataFrame,
    tags_df: pd.DataFrame,
    max_features: int = 20000,
    svd_dim: int = 100,
) -> ContentArtifacts:
    if movies_df.empty:
        return ContentArtifacts(movie_ids=[], vectors=np.empty((0, 0), dtype="float32"))

    tags_grouped = (
        tags_df.dropna(subset=["tag"])
        .groupby("movieId")["tag"]
        .apply(lambda items: " ".join(sorted(set(items))))
    )

    movies_df = movies_df.copy()
    movies_df["genres"] = movies_df["genres"].fillna("")
    movies_df["tag_text"] = movies_df["movieId"].map(tags_grouped).fillna("")
    movies_df["text"] = (
        movies_df["title"].fillna("")
        + " "
        + movies_df["genres"].astype(str)
        + " "
        + movies_df["tag_text"].astype(str)
    )

    texts = movies_df["text"].tolist()
    vectorizer = TfidfVectorizer(
        max_features=max_features, stop_words="english", min_df=2
    )
    try:
        tfidf = vectorizer.fit_transform(texts)
    except ValueError:
        vectorizer = TfidfVectorizer(
            max_features=max_features, stop_words="english", min_df=1
        )
        tfidf = vectorizer.fit_transform(texts)

    if tfidf.shape[1] <= 1:
        vectors = normalize(tfidf.toarray())
    else:
        effective_dim = min(svd_dim, tfidf.shape[1] - 1)
        svd = TruncatedSVD(n_components=effective_dim, random_state=42)
        vectors = normalize(svd.fit_transform(tfidf))

    return ContentArtifacts(
        movie_ids=movies_df["movieId"].astype("int32").tolist(),
        vectors=vectors.astype("float32"),
    )


def build_annoy_index(vectors: np.ndarray, n_trees: int = 50):
    if AnnoyIndex is None or vectors.size == 0 or vectors.ndim != 2:
        return None
    dim = vectors.shape[1]
    if dim == 0:
        return None
    index = AnnoyIndex(dim, "angular")
    for idx, vec in enumerate(vectors):
        index.add_item(idx, vec.astype("float32").tolist())
    index.build(n_trees)
    return index


def save_neighbors_jsonl(
    path: Path, neighbors: Dict[int, List[Tuple[int, float]]], id_field: str
) -> None:
    _ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for source_id, items in neighbors.items():
            payload = {
                id_field: int(source_id),
                "neighbors": [
                    {id_field: int(target_id), "score": float(score)}
                    for target_id, score in items
                ],
            }
            handle.write(json.dumps(payload) + "\n")


def load_neighbors_jsonl(path: Path, id_field: str) -> Dict[int, List[Tuple[int, float]]]:
    neighbors: Dict[int, List[Tuple[int, float]]] = {}
    if not path.exists():
        return neighbors
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            source_id = int(payload[id_field])
            items = payload.get("neighbors", [])
            neighbors[source_id] = [
                (int(item[id_field]), float(item["score"])) for item in items
            ]
    return neighbors


def save_content_artifacts(path: Path, content: ContentArtifacts) -> None:
    _ensure_dir(path)
    np.save(path / "content_vectors.npy", content.vectors)
    with (path / "content_movie_ids.json").open("w", encoding="utf-8") as handle:
        json.dump(content.movie_ids, handle)


def load_content_artifacts(path: Path) -> ContentArtifacts:
    vectors_path = path / "content_vectors.npy"
    ids_path = path / "content_movie_ids.json"
    if not vectors_path.exists() or not ids_path.exists():
        raise FileNotFoundError(
            "Missing content artifacts. Run `build-artifacts` before "
            "`generate-candidates`."
        )

    vectors = np.load(vectors_path)
    with ids_path.open("r", encoding="utf-8") as handle:
        movie_ids = json.load(handle)
    if len(movie_ids) != len(vectors):
        raise ValueError(
            "Content artifact mismatch: content_movie_ids.json length does not "
            "match content_vectors.npy rows."
        )
    return ContentArtifacts(movie_ids=movie_ids, vectors=vectors)


def save_popularity(path: Path, popularity_df: pd.DataFrame) -> None:
    _ensure_dir(path.parent)
    popularity_df.to_csv(path, index=False)


def load_popularity(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing popularity artifact: {path}. Run `build-artifacts` first."
        )
    return pd.read_csv(path)


def _normalize_scores(scores: Dict[int, float]) -> Dict[int, float]:
    if not scores:
        return scores
    values = np.array(list(scores.values()), dtype="float32")
    min_val = float(values.min())
    max_val = float(values.max())
    if max_val <= min_val:
        return {key: 0.0 for key in scores}
    return {
        key: (float(value) - min_val) / (max_val - min_val)
        for key, value in scores.items()
    }


def _build_user_history(ratings_df: pd.DataFrame) -> Dict[int, List[int]]:
    history: Dict[int, List[int]] = {}
    for user_id, group in ratings_df.groupby("userId"):
        history[int(user_id)] = (
            group.sort_values("timestamp")["movieId"].astype("int32").tolist()
        )
    return history


def generate_candidates(
    ratings_df: pd.DataFrame,
    popularity_df: pd.DataFrame,
    item_neighbors: Dict[int, List[Tuple[int, float]]],
    user_neighbors: Dict[int, List[Tuple[int, float]]],
    content: ContentArtifacts,
    max_users: Optional[int],
    pop_k: int,
    item_k: int,
    user_k: int,
    user_item_k: int,
    content_k: int,
    weights: Dict[str, float],
) -> pd.DataFrame:
    ratings_df = ratings_df.copy()
    ratings_df["timestamp"] = _parse_timestamp(ratings_df["timestamp"])
    ratings_df = ratings_df.dropna(subset=["timestamp"])

    user_history = _build_user_history(ratings_df)
    user_ids = list(user_history.keys())
    if max_users is not None:
        user_ids = user_ids[:max_users]

    pop_scores = dict(zip(popularity_df["movieId"], popularity_df["score_popularity"]))
    pop_scores = _normalize_scores(pop_scores)
    pop_top = sorted(pop_scores.items(), key=lambda item: item[1], reverse=True)[:pop_k]

    content_index = {movie_id: idx for idx, movie_id in enumerate(content.movie_ids)}
    annoy_index = build_annoy_index(content.vectors, n_trees=50)
    fallback_knn = None
    if annoy_index is None and content.vectors.size > 0:
        n_neighbors = min(content_k + 1, content.vectors.shape[0])
        if n_neighbors > 0:
            fallback_knn = NearestNeighbors(
                metric="cosine", algorithm="brute", n_neighbors=n_neighbors
            )
            fallback_knn.fit(content.vectors)

    records: List[Dict[str, float]] = []
    for user_id in user_ids:
        seen_items = set(user_history.get(user_id, []))
        candidates: Dict[int, Dict[str, float]] = {}

        for movie_id, score in pop_top:
            if movie_id in seen_items:
                continue
            candidates.setdefault(movie_id, {})["score_pop"] = score

        for movie_id in seen_items:
            for neighbor_id, score in item_neighbors.get(movie_id, [])[:item_k]:
                if neighbor_id in seen_items:
                    continue
                entry = candidates.setdefault(neighbor_id, {})
                entry["score_item"] = max(score, entry.get("score_item", 0.0))

        for neighbor_user, score in user_neighbors.get(user_id, [])[:user_k]:
            for neighbor_item in user_history.get(neighbor_user, [])[:user_item_k]:
                if neighbor_item in seen_items:
                    continue
                entry = candidates.setdefault(neighbor_item, {})
                entry["score_user"] = max(score, entry.get("score_user", 0.0))

        profile_vectors = [
            content.vectors[content_index[item_id]]
            for item_id in seen_items
            if item_id in content_index
        ]
        if profile_vectors:
            profile = normalize(np.mean(profile_vectors, axis=0).reshape(1, -1))[0]
            if annoy_index is not None:
                ids, distances = annoy_index.get_nns_by_vector(
                    profile.tolist(), content_k, include_distances=True
                )
                for idx, dist in zip(ids, distances):
                    movie_id = content.movie_ids[idx]
                    if movie_id in seen_items:
                        continue
                    score = max(0.0, 1.0 - (dist * dist) / 2.0)
                    entry = candidates.setdefault(movie_id, {})
                    entry["score_content"] = max(score, entry.get("score_content", 0.0))
            elif fallback_knn is not None:
                distances, indices = fallback_knn.kneighbors(profile.reshape(1, -1))
                for idx, dist in zip(indices[0], distances[0]):
                    movie_id = content.movie_ids[int(idx)]
                    if movie_id in seen_items:
                        continue
                    score = max(0.0, 1.0 - float(dist))
                    entry = candidates.setdefault(movie_id, {})
                    entry["score_content"] = max(score, entry.get("score_content", 0.0))

        for movie_id, scores in candidates.items():
            score_pop = scores.get("score_pop", 0.0)
            score_item = scores.get("score_item", 0.0)
            score_user = scores.get("score_user", 0.0)
            score_content = scores.get("score_content", 0.0)
            score_final = (
                weights["pop"] * score_pop
                + weights["item"] * score_item
                + weights["user"] * score_user
                + weights["content"] * score_content
            )
            records.append(
                {
                    "userId": int(user_id),
                    "movieId": int(movie_id),
                    "score_pop": float(score_pop),
                    "score_item": float(score_item),
                    "score_user": float(score_user),
                    "score_content": float(score_content),
                    "score_final": float(score_final),
                }
            )

    return pd.DataFrame.from_records(records, columns=CANDIDATE_COLUMNS)


def evaluate_candidates(
    candidates_df: pd.DataFrame,
    test_df: pd.DataFrame,
    movies_df: pd.DataFrame,
    k: int,
) -> Dict[str, float]:
    if candidates_df.empty or test_df.empty:
        return {
            "hit_rate": 0.0,
            "ndcg": 0.0,
            "coverage": 0.0,
            "diversity": 0.0,
        }

    test_items = test_df.groupby("userId")["movieId"].apply(set).to_dict()
    recs = (
        candidates_df.sort_values("score_final", ascending=False)
        .groupby("userId")
        .head(k)
    )

    hits = []
    ndcgs = []
    for user_id, group in recs.groupby("userId"):
        items = group["movieId"].tolist()
        truth = test_items.get(user_id, set())
        if not truth:
            continue
        hit = 1.0 if any(item in truth for item in items) else 0.0
        hits.append(hit)

        dcg = 0.0
        for idx, item in enumerate(items):
            if item in truth:
                dcg += 1.0 / np.log2(idx + 2)
        idcg = sum(1.0 / np.log2(i + 2) for i in range(min(len(truth), k)))
        ndcgs.append(dcg / idcg if idcg > 0 else 0.0)

    unique_recs = recs["movieId"].nunique()
    total_items = movies_df["movieId"].nunique() if not movies_df.empty else unique_recs
    coverage = unique_recs / total_items if total_items else 0.0

    genre_map = {
        row.movieId: set(str(row.genres).split("|")) if pd.notna(row.genres) else set()
        for row in movies_df.itertuples()
    }
    diversity_scores = []
    for _, group in recs.groupby("userId"):
        items = group["movieId"].tolist()
        if len(items) < 2:
            continue
        pair_scores = []
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                a = genre_map.get(items[i], set())
                b = genre_map.get(items[j], set())
                if not a and not b:
                    continue
                jaccard = len(a & b) / len(a | b) if (a | b) else 0.0
                pair_scores.append(jaccard)
        if pair_scores:
            diversity_scores.append(1.0 - float(np.mean(pair_scores)))

    return {
        "hit_rate": float(np.mean(hits)) if hits else 0.0,
        "ndcg": float(np.mean(ndcgs)) if ndcgs else 0.0,
        "coverage": float(coverage),
        "diversity": float(np.mean(diversity_scores)) if diversity_scores else 0.0,
    }
