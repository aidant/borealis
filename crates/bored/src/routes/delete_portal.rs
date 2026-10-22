use axum::{Json, Router, extract::Path, routing::delete};

pub async fn delete_portal(Path(_portal_id): Path<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

pub fn init(router: Router) -> Router {
    router.route("/api/v1/portal/{portal_id}", delete(delete_portal))
}
