from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from backend.app.application.use_cases.phase04.pipeline import (
    CANDIDATE_COLUMNS,
    build_annoy_index,
    build_content_embeddings,
    build_interaction_neighbors,
    build_popularity,
    evaluate_candidates,
    generate_candidates,
    load_content_artifacts,
    load_movies,
    load_popularity,
    load_ratings,
    load_tags,
    load_neighbors_jsonl,
    save_content_artifacts,
    save_neighbors_jsonl,
    save_popularity,
    split_train_test_by_time,
)


PHASE04_ARTIFACTS = [
    "popularity.csv",
    "item_neighbors.jsonl",
    "user_neighbors.jsonl",
    "content_vectors.npy",
    "content_movie_ids.json",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def _default_paths() -> dict:
    root = _repo_root()
    data_raw = _default_data_dir(root)
    output_dir = root / "data" / "models" / "phase04"
    return {
        "movies": data_raw / "movie.csv",
        "ratings": data_raw / "rating.csv",
        "tags": data_raw / "tag.csv",
        "output": output_dir,
    }


def _default_data_dir(root: Path) -> Path:
    required = ["movie.csv", "rating.csv", "tag.csv"]
    for candidate in (root / "data" / "raw", root / "data" / "archive"):
        if all((candidate / name).exists() for name in required):
            return candidate
    return root / "data" / "raw"


def _parse_weights(args: argparse.Namespace) -> dict:
    return {
        "pop": args.w_pop,
        "item": args.w_item,
        "user": args.w_user,
        "content": args.w_content,
    }


def _require_file(path: str | Path, label: str) -> Path:
    resolved = Path(path)
    if not resolved.exists():
        raise FileNotFoundError(f"Missing {label}: {resolved}")
    if not resolved.is_file():
        raise FileNotFoundError(f"Expected {label} to be a file: {resolved}")
    return resolved


def _require_artifacts(output_dir: Path) -> None:
    missing = [name for name in PHASE04_ARTIFACTS if not (output_dir / name).exists()]
    if missing:
        missing_list = ", ".join(missing)
        raise FileNotFoundError(
            "Missing Phase 4 artifacts in "
            f"{output_dir}: {missing_list}. Run `build-artifacts` first."
        )


def build_artifacts(args: argparse.Namespace) -> None:
    _require_file(args.movies, "movies input")
    _require_file(args.ratings, "ratings input")
    _require_file(args.tags, "tags input")

    movies = load_movies(args.movies, max_rows=args.max_movies)
    ratings = load_ratings(args.ratings, max_rows=args.max_ratings)
    tags = load_tags(args.tags, max_rows=args.max_tags)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    popularity = build_popularity(ratings, decay=args.decay)
    save_popularity(output_dir / "popularity.csv", popularity)

    item_neighbors, user_neighbors = build_interaction_neighbors(
        ratings,
        top_k_item=args.item_k,
        top_k_user=args.user_k,
        min_rating=args.min_rating,
    )
    save_neighbors_jsonl(
        output_dir / "item_neighbors.jsonl", item_neighbors, "movieId"
    )
    save_neighbors_jsonl(
        output_dir / "user_neighbors.jsonl", user_neighbors, "userId"
    )

    content = build_content_embeddings(
        movies,
        tags,
        max_features=args.max_features,
        svd_dim=args.svd_dim,
    )
    save_content_artifacts(output_dir, content)

    annoy_index = build_annoy_index(content.vectors, n_trees=args.annoy_trees)
    if annoy_index is not None:
        annoy_index.save(str(output_dir / "content_annoy.ann"))


def build_candidates(args: argparse.Namespace) -> None:
    output_dir = Path(args.output)
    _require_file(args.ratings, "ratings input")
    _require_artifacts(output_dir)

    popularity = load_popularity(output_dir / "popularity.csv")
    item_neighbors = load_neighbors_jsonl(output_dir / "item_neighbors.jsonl", "movieId")
    user_neighbors = load_neighbors_jsonl(output_dir / "user_neighbors.jsonl", "userId")
    content = load_content_artifacts(output_dir)

    ratings = load_ratings(args.ratings, max_rows=args.max_ratings)
    train_df, test_df = split_train_test_by_time(ratings, holdout=args.holdout)
    test_df.to_csv(output_dir / "test_holdout.csv", index=False)

    weights = _parse_weights(args)
    candidates_df = generate_candidates(
        train_df,
        popularity,
        item_neighbors,
        user_neighbors,
        content,
        max_users=args.max_users,
        pop_k=args.pop_k,
        item_k=args.item_k,
        user_k=args.user_k,
        user_item_k=args.user_item_k,
        content_k=args.content_k,
        weights=weights,
    )
    candidates_df = candidates_df.reindex(columns=CANDIDATE_COLUMNS)
    candidates_df.to_csv(output_dir / "candidates.csv", index=False)


def build_evaluation(args: argparse.Namespace) -> None:
    output_dir = Path(args.output)
    if args.candidates:
        candidates_path = Path(args.candidates)
    else:
        candidates_path = output_dir / "candidates.csv"
    if not candidates_path.exists():
        raise FileNotFoundError(
            f"Missing candidates file: {candidates_path}. Run "
            "`generate-candidates` before `evaluate`."
        )

    _require_file(args.ratings, "ratings input")
    _require_file(args.movies, "movies input")
    ratings = load_ratings(args.ratings, max_rows=args.max_ratings)
    movies = load_movies(args.movies, max_rows=args.max_movies)

    test_path = Path(args.test)
    if test_path.exists():
        test_df = load_ratings(str(test_path))
    else:
        _, test_df = split_train_test_by_time(ratings, holdout=args.holdout)

    candidates_df = pd.read_csv(candidates_path)
    metrics = evaluate_candidates(candidates_df, test_df, movies, k=args.k)

    report_path = output_dir / "evaluation.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2)


def _build_parser() -> argparse.ArgumentParser:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(description="Phase 4 hybrid recommendation pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build-artifacts", help="Build Phase 4 artifacts")
    build_parser.add_argument("--movies", default=str(defaults["movies"]))
    build_parser.add_argument("--ratings", default=str(defaults["ratings"]))
    build_parser.add_argument("--tags", default=str(defaults["tags"]))
    build_parser.add_argument("--output", default=str(defaults["output"]))
    build_parser.add_argument("--max-movies", type=int, default=None)
    build_parser.add_argument("--max-ratings", type=int, default=None)
    build_parser.add_argument("--max-tags", type=int, default=None)
    build_parser.add_argument("--decay", type=float, default=0.01)
    build_parser.add_argument("--item-k", type=int, default=50)
    build_parser.add_argument("--user-k", type=int, default=30)
    build_parser.add_argument("--min-rating", type=float, default=None)
    build_parser.add_argument("--max-features", type=int, default=20000)
    build_parser.add_argument("--svd-dim", type=int, default=100)
    build_parser.add_argument("--annoy-trees", type=int, default=50)
    build_parser.set_defaults(func=build_artifacts)

    candidates_parser = subparsers.add_parser(
        "generate-candidates", help="Generate candidates and holdout split"
    )
    candidates_parser.add_argument("--ratings", default=str(defaults["ratings"]))
    candidates_parser.add_argument("--output", default=str(defaults["output"]))
    candidates_parser.add_argument("--max-ratings", type=int, default=None)
    candidates_parser.add_argument("--max-users", type=int, default=1000)
    candidates_parser.add_argument("--holdout", type=int, default=1)
    candidates_parser.add_argument("--pop-k", type=int, default=200)
    candidates_parser.add_argument("--item-k", type=int, default=50)
    candidates_parser.add_argument("--user-k", type=int, default=20)
    candidates_parser.add_argument("--user-item-k", type=int, default=20)
    candidates_parser.add_argument("--content-k", type=int, default=200)
    candidates_parser.add_argument("--w-pop", type=float, default=0.2)
    candidates_parser.add_argument("--w-item", type=float, default=0.4)
    candidates_parser.add_argument("--w-user", type=float, default=0.2)
    candidates_parser.add_argument("--w-content", type=float, default=0.2)
    candidates_parser.set_defaults(func=build_candidates)

    eval_parser = subparsers.add_parser("evaluate", help="Evaluate candidates offline")
    eval_parser.add_argument("--movies", default=str(defaults["movies"]))
    eval_parser.add_argument("--ratings", default=str(defaults["ratings"]))
    eval_parser.add_argument("--output", default=str(defaults["output"]))
    eval_parser.add_argument("--candidates", default="")
    eval_parser.add_argument("--test", default=str(defaults["output"] / "test_holdout.csv"))
    eval_parser.add_argument("--max-movies", type=int, default=None)
    eval_parser.add_argument("--max-ratings", type=int, default=None)
    eval_parser.add_argument("--holdout", type=int, default=1)
    eval_parser.add_argument("--k", type=int, default=10)
    eval_parser.set_defaults(func=build_evaluation)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
