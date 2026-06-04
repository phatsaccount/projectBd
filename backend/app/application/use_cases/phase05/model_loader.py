"""Model artifact loader for Phase 5."""

import json
import logging
from pathlib import Path
from typing import Dict, Optional, List, Tuple
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class Phase4ArtifactLoader:
    """Load Phase 4 model artifacts for recommendation serving."""
    
    def __init__(self, artifacts_dir: Path):
        """Initialize artifact loader.
        
        Args:
            artifacts_dir: Path to Phase 4 artifacts directory
        """
        self.artifacts_dir = Path(artifacts_dir)
        if not self.artifacts_dir.exists():
            raise FileNotFoundError(f"Artifacts directory not found: {artifacts_dir}")
        
        self._artifacts_cache = {}
        logger.info(f"Phase4ArtifactLoader initialized with {artifacts_dir}")
    
    def load_popularity(self) -> Optional[pd.DataFrame]:
        """Load popularity scores.
        
        Returns:
            DataFrame with columns: [movie_id, popularity_score]
        """
        if "popularity" in self._artifacts_cache:
            return self._artifacts_cache["popularity"]
        
        try:
            path = self.artifacts_dir / "popularity.csv"
            if not path.exists():
                logger.warning(f"Popularity file not found: {path}")
                return None
            
            df = pd.read_csv(path)
            self._artifacts_cache["popularity"] = df
            logger.info(f"Loaded popularity: {len(df)} movies")
            return df
        except Exception as e:
            logger.error(f"Error loading popularity: {e}")
            return None
    
    def load_item_neighbors(self) -> Optional[Dict[int, List[Tuple[int, float]]]]:
        """Load item-item neighbors.
        
        Returns:
            Dict mapping movie_id to list of (neighbor_id, score) tuples
        """
        if "item_neighbors" in self._artifacts_cache:
            return self._artifacts_cache["item_neighbors"]
        
        try:
            path = self.artifacts_dir / "item_neighbors.jsonl"
            if not path.exists():
                logger.warning(f"Item neighbors file not found: {path}")
                return {}
            
            neighbors = {}
            with open(path, "r") as f:
                for line in f:
                    record = json.loads(line)
                    movie_id = record.get("movie_id", record.get("movieId"))
                    if movie_id is None:
                        continue
                    neighbors[movie_id] = [
                        (int(n.get("neighbor_id", n.get("movie_id", n.get("movieId")))), float(n["score"]))
                        for n in record.get("neighbors", [])
                        if n.get("neighbor_id", n.get("movie_id", n.get("movieId"))) is not None
                    ]
            
            self._artifacts_cache["item_neighbors"] = neighbors
            logger.info(f"Loaded item neighbors: {len(neighbors)} movies")
            return neighbors
        except Exception as e:
            logger.error(f"Error loading item neighbors: {e}")
            return {}
    
    def load_user_neighbors(self) -> Optional[Dict[int, List[Tuple[int, float]]]]:
        """Load user-user neighbors.
        
        Returns:
            Dict mapping user_id to list of (neighbor_id, score) tuples
        """
        if "user_neighbors" in self._artifacts_cache:
            return self._artifacts_cache["user_neighbors"]
        
        try:
            path = self.artifacts_dir / "user_neighbors.jsonl"
            if not path.exists():
                logger.warning(f"User neighbors file not found: {path}")
                return {}
            
            neighbors = {}
            with open(path, "r") as f:
                for line in f:
                    record = json.loads(line)
                    user_id = record.get("user_id", record.get("userId"))
                    if user_id is None:
                        continue
                    neighbors[user_id] = [
                        (int(n.get("neighbor_id", n.get("user_id", n.get("userId")))), float(n["score"]))
                        for n in record.get("neighbors", [])
                        if n.get("neighbor_id", n.get("user_id", n.get("userId"))) is not None
                    ]
            
            self._artifacts_cache["user_neighbors"] = neighbors
            logger.info(f"Loaded user neighbors: {len(neighbors)} users")
            return neighbors
        except Exception as e:
            logger.error(f"Error loading user neighbors: {e}")
            return {}
    
    def load_content_vectors(self) -> Optional[np.ndarray]:
        """Load content-based embeddings.
        
        Returns:
            NumPy array of content vectors
        """
        if "content_vectors" in self._artifacts_cache:
            return self._artifacts_cache["content_vectors"]
        
        try:
            path = self.artifacts_dir / "content_vectors.npy"
            if not path.exists():
                logger.warning(f"Content vectors file not found: {path}")
                return None
            
            vectors = np.load(path)
            self._artifacts_cache["content_vectors"] = vectors
            logger.info(f"Loaded content vectors: shape {vectors.shape}")
            return vectors
        except Exception as e:
            logger.error(f"Error loading content vectors: {e}")
            return None
    
    def load_content_movie_ids(self) -> Optional[Dict[int, int]]:
        """Load mapping of array indices to movie IDs.
        
        Returns:
            Dict mapping array index to movie_id
        """
        if "content_movie_ids" in self._artifacts_cache:
            return self._artifacts_cache["content_movie_ids"]
        
        try:
            path = self.artifacts_dir / "content_movie_ids.json"
            if not path.exists():
                logger.warning(f"Content movie IDs file not found: {path}")
                return {}
            
            with open(path, "r") as f:
                loaded = json.load(f)
            
            if isinstance(loaded, dict):
                mapping = {int(k): int(v) for k, v in loaded.items()}
            elif isinstance(loaded, list):
                mapping = {idx: int(movie_id) for idx, movie_id in enumerate(loaded)}
            else:
                raise ValueError(
                    f"Unsupported content movie IDs format: {type(loaded).__name__}"
                )

            self._artifacts_cache["content_movie_ids"] = mapping
            logger.info(f"Loaded content movie IDs: {len(mapping)} mappings")
            return mapping
        except Exception as e:
            logger.error(f"Error loading content movie IDs: {e}")
            return {}
    
    def load_all_artifacts(self) -> Dict[str, any]:
        """Load all Phase 4 artifacts.
        
        Returns:
            Dict with all loaded artifacts
        """
        return {
            "popularity": self.load_popularity(),
            "item_neighbors": self.load_item_neighbors(),
            "user_neighbors": self.load_user_neighbors(),
            "content_vectors": self.load_content_vectors(),
            "content_movie_ids": self.load_content_movie_ids(),
        }
    
    def verify_artifacts(self) -> bool:
        """Verify that all required artifacts exist.
        
        Returns:
            True if all artifacts are present
        """
        required_files = [
            "popularity.csv",
            "item_neighbors.jsonl",
            "user_neighbors.jsonl",
            "content_vectors.npy",
            "content_movie_ids.json",
        ]
        
        missing = [f for f in required_files if not (self.artifacts_dir / f).exists()]
        
        if missing:
            logger.error(f"Missing Phase 4 artifacts: {missing}")
            return False
        
        logger.info("All Phase 4 artifacts verified")
        return True
