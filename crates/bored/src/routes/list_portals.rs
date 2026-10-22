use axum::{Json, Router, routing::get};

pub async fn list_portals() -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

pub fn init(router: Router) -> Router {
    router.route("/api/v1/portal", get(list_portals))
}
