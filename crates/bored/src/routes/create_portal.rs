use axum::{Json, Router, routing::post};

pub async fn create_portal() -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

pub fn init(router: Router) -> Router {
    router.route("/api/v1/portal", post(create_portal))
}
