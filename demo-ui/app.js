const state = {
  apiBaseUrl: "http://localhost:8000",
  currentUserId: 1,
};

const elements = {
  apiBaseUrl: document.querySelector("#apiBaseUrl"),
  searchForm: document.querySelector("#searchForm"),
  recommendationForm: document.querySelector("#recommendationForm"),
  query: document.querySelector("#query"),
  page: document.querySelector("#page"),
  size: document.querySelector("#size"),
  userId: document.querySelector("#userId"),
  recommendationCount: document.querySelector("#recommendationCount"),
  status: document.querySelector("#status"),
  searchResults: document.querySelector("#searchResults"),
  searchTotal: document.querySelector("#searchTotal"),
  movieDetail: document.querySelector("#movieDetail"),
  recommendationResults: document.querySelector("#recommendationResults"),
  recommendationMeta: document.querySelector("#recommendationMeta"),
};

function setStatus(message, type = "info") {
  elements.status.textContent = message || "";
  elements.status.classList.toggle("error", type === "error");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function getApiBaseUrl() {
  return elements.apiBaseUrl.value.trim().replace(/\/$/, "") || state.apiBaseUrl;
}

async function apiFetch(path, options = {}) {
  const response = await fetch(`${getApiBaseUrl()}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      detail = body.detail || body.error || JSON.stringify(body);
    } catch {
      detail = await response.text();
    }
    throw new Error(`${response.status} ${detail}`);
  }

  return response.json();
}

function genreText(movie) {
  if (Array.isArray(movie.genres_list) && movie.genres_list.length) {
    return movie.genres_list.join(", ");
  }
  return movie.genres || "Unknown genre";
}

function tagHtml(tags) {
  if (!Array.isArray(tags) || !tags.length) {
    return "";
  }
  return `
    <div class="tags">
      ${tags.slice(0, 6).map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join("")}
    </div>
  `;
}

function movieCard(movie, source = "search") {
  const movieId = movie.movieId ?? movie.movie_id;
  return `
    <article class="movie-card">
      <h3>${escapeHtml(movie.title || `Movie #${movieId}`)}</h3>
      <p class="meta">ID ${escapeHtml(movieId)} · ${escapeHtml(genreText(movie))}</p>
      <p class="meta">
        Rating: ${movie.rating_mean ?? "n/a"} · Count: ${movie.rating_count ?? "n/a"}
      </p>
      ${tagHtml(movie.tag_list)}
      <div class="card-actions">
        <button type="button" data-detail="${escapeHtml(movieId)}">Details</button>
        <button type="button" class="secondary" data-event="view" data-movie="${escapeHtml(movieId)}">View</button>
        <button type="button" class="secondary" data-event="bookmark" data-movie="${escapeHtml(movieId)}">Bookmark</button>
        <button type="button" class="secondary" data-event="share" data-movie="${escapeHtml(movieId)}">Share</button>
        <button type="button" class="warning" data-event="rate" data-movie="${escapeHtml(movieId)}">Rate 5</button>
      </div>
      <p class="meta">Source: ${escapeHtml(source)}</p>
    </article>
  `;
}

function renderSearchResults(response) {
  const hits = response.hits || [];
  elements.searchTotal.textContent = `${response.total || 0} movies`;
  if (!hits.length) {
    elements.searchResults.className = "movie-grid empty-state";
    elements.searchResults.textContent = "No movies matched this query.";
    return;
  }

  elements.searchResults.className = "movie-grid";
  elements.searchResults.innerHTML = hits.map((movie) => movieCard(movie)).join("");
}

function renderMovieDetail(movie) {
  elements.movieDetail.className = "detail-card";
  elements.movieDetail.innerHTML = `
    <h3>${escapeHtml(movie.title || `Movie #${movie.movieId}`)}</h3>
    <p class="meta">Movie ID: ${escapeHtml(movie.movieId)}</p>
    <p class="meta">Genres: ${escapeHtml(genreText(movie))}</p>
    <p class="meta">Rating mean: ${movie.rating_mean ?? "n/a"}</p>
    <p class="meta">Rating count: ${movie.rating_count ?? "n/a"}</p>
    ${tagHtml(movie.tag_list)}
    <div class="card-actions">
      <button type="button" class="secondary" data-event="view" data-movie="${escapeHtml(movie.movieId)}">View</button>
      <button type="button" class="warning" data-event="rate" data-movie="${escapeHtml(movie.movieId)}">Rate 5</button>
    </div>
  `;
}

async function loadMovieDetail(movieId) {
  setStatus(`Loading movie #${movieId}...`);
  const movie = await apiFetch(`/movies/${encodeURIComponent(movieId)}`);
  renderMovieDetail(movie);
  setStatus(`Loaded details for ${movie.title || `movie #${movieId}`}.`);
}

async function enrichRecommendation(item) {
  try {
    const movie = await apiFetch(`/movies/${encodeURIComponent(item.movie_id)}`);
    return { ...item, movie };
  } catch {
    return { ...item, movie: null };
  }
}

function renderRecommendations(response, enrichedItems) {
  elements.recommendationMeta.textContent = `${response.total || 0} movies · cached: ${response.cached ? "yes" : "no"}`;
  if (!enrichedItems.length) {
    elements.recommendationResults.className = "recommendation-list empty-state";
    elements.recommendationResults.textContent = "No recommendations returned for this user.";
    return;
  }

  elements.recommendationResults.className = "recommendation-list";
  elements.recommendationResults.innerHTML = enrichedItems
    .map((item) => {
      const title = item.movie?.title || `Movie #${item.movie_id}`;
      const genres = item.movie ? genreText(item.movie) : "Details unavailable";
      return `
        <article class="recommendation-card">
          <div class="rank">${escapeHtml(item.rank)}</div>
          <div>
            <h3>${escapeHtml(title)}</h3>
            <p class="meta">${escapeHtml(genres)}</p>
            <p class="meta">Reason: ${escapeHtml(item.reason)} · Score: ${Number(item.score).toFixed(4)}</p>
          </div>
          <div class="card-actions">
            <button type="button" data-detail="${escapeHtml(item.movie_id)}">Details</button>
            <button type="button" class="secondary" data-event="view" data-movie="${escapeHtml(item.movie_id)}">View</button>
            <button type="button" class="warning" data-event="rate" data-movie="${escapeHtml(item.movie_id)}">Rate 5</button>
          </div>
        </article>
      `;
    })
    .join("");
}

async function trackEvent(movieId, eventType) {
  const payload = {
    user_id: state.currentUserId,
    movie_id: Number(movieId),
    event_type: eventType,
  };
  if (eventType === "rate") {
    payload.rating = 5;
  }

  setStatus(`Sending ${eventType} event for movie #${movieId}...`);
  const response = await apiFetch("/events/track", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  setStatus(`Event accepted: ${response.event_id}`);
}

async function handleSearch(event) {
  event.preventDefault();
  const params = new URLSearchParams({
    q: elements.query.value.trim(),
    page: elements.page.value || "1",
    size: elements.size.value || "10",
  });

  setStatus("Searching movies...");
  elements.searchResults.className = "movie-grid empty-state";
  elements.searchResults.textContent = "Loading search results...";

  try {
    const response = await apiFetch(`/movies/search?${params.toString()}`);
    renderSearchResults(response);
    setStatus("Search complete.");
  } catch (error) {
    setStatus(`Search failed: ${error.message}. Check that the backend and Elasticsearch are running.`, "error");
  }
}

async function handleRecommendations(event) {
  event.preventDefault();
  state.currentUserId = Number(elements.userId.value || 1);
  const k = elements.recommendationCount.value || "10";

  setStatus(`Loading recommendations for user #${state.currentUserId}...`);
  elements.recommendationResults.className = "recommendation-list empty-state";
  elements.recommendationResults.textContent = "Loading recommendations...";

  try {
    const response = await apiFetch(`/recommendations/${encodeURIComponent(state.currentUserId)}?k=${encodeURIComponent(k)}`);
    const enrichedItems = await Promise.all((response.recommendations || []).map(enrichRecommendation));
    renderRecommendations(response, enrichedItems);
    setStatus("Recommendations loaded.");
  } catch (error) {
    setStatus(`Recommendations failed: ${error.message}. Check that the backend and Redis/artifacts are ready.`, "error");
  }
}

async function handleDocumentClick(event) {
  const detailButton = event.target.closest("[data-detail]");
  const eventButton = event.target.closest("[data-event]");

  try {
    if (detailButton) {
      await loadMovieDetail(detailButton.dataset.detail);
    }
    if (eventButton) {
      state.currentUserId = Number(elements.userId.value || 1);
      await trackEvent(eventButton.dataset.movie, eventButton.dataset.event);
    }
  } catch (error) {
    setStatus(`Action failed: ${error.message}. Check that backend, Kafka, and dependencies are running.`, "error");
  }
}

elements.apiBaseUrl.addEventListener("change", () => {
  state.apiBaseUrl = getApiBaseUrl();
});
elements.searchForm.addEventListener("submit", handleSearch);
elements.recommendationForm.addEventListener("submit", handleRecommendations);
document.addEventListener("click", handleDocumentClick);
