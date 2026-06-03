import pandas as pd

from backend.app.application.use_cases.phase04.pipeline import (
    build_content_embeddings,
    build_interaction_neighbors,
    build_popularity,
    evaluate_candidates,
    generate_candidates,
    split_train_test_by_time,
)


def main() -> None:
    movies = pd.DataFrame(
        {
            "movieId": [1, 2, 3, 4, 5],
            "title": [
                "Toy Story",
                "Jumanji",
                "Heat",
                "GoldenEye",
                "Casino",
            ],
            "genres": [
                "Adventure|Animation|Children",
                "Adventure|Fantasy",
                "Crime|Thriller",
                "Action|Thriller",
                "Crime|Drama",
            ],
        }
    )

    ratings = pd.DataFrame(
        {
            "userId": [1, 1, 1, 2, 2, 2, 3, 3, 3],
            "movieId": [1, 2, 3, 2, 3, 4, 1, 4, 5],
            "rating": [4.0, 3.5, 5.0, 4.0, 4.5, 3.0, 4.0, 3.5, 4.0],
            "timestamp": [
                1600000000,
                1600001000,
                1600002000,
                1600000000,
                1600001000,
                1600002000,
                1600000000,
                1600001000,
                1600002000,
            ],
        }
    )

    tags = pd.DataFrame(
        {
            "userId": [1, 2, 3],
            "movieId": [1, 3, 4],
            "tag": ["fun", "heist", "spy"],
            "timestamp": ["2020-09-13", "2020-09-14", "2020-09-15"],
        }
    )

    popularity = build_popularity(ratings)
    item_neighbors, user_neighbors = build_interaction_neighbors(
        ratings, top_k_item=3, top_k_user=2, min_rating=None
    )
    content = build_content_embeddings(movies, tags, max_features=100, svd_dim=2)
    train_df, test_df = split_train_test_by_time(ratings, holdout=1)
    weights = {"pop": 0.2, "item": 0.4, "user": 0.2, "content": 0.2}

    candidates = generate_candidates(
        train_df,
        popularity,
        item_neighbors,
        user_neighbors,
        content,
        max_users=None,
        pop_k=3,
        item_k=3,
        user_k=2,
        user_item_k=2,
        content_k=3,
        weights=weights,
    )

    assert not candidates.empty, "Expected non-empty candidates"
    metrics = evaluate_candidates(candidates, test_df, movies, k=3)
    assert "hit_rate" in metrics
    print("Smoke test metrics:", metrics)


if __name__ == "__main__":
    main()
