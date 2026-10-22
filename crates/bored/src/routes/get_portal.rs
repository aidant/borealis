use axum::{Json, Router, extract::Path, routing::get};

pub async fn get_portal(Path(_portal_id): Path<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

pub fn init(router: Router) -> Router {
    router.route("/api/v1/portal/{portal_id}", get(get_portal))
}
