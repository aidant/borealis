use axum::{Json, Router, extract::Path, routing::post};

pub async fn portal_connect(Path(_portal_id): Path<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

pub fn init(router: Router) -> Router {
    router.route("/api/v1/portal/{portal_id}/connect", post(portal_connect))
}
