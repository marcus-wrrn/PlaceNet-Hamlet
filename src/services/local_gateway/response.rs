use std::convert::Infallible;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::Response;

use super::internals::ProxyBody;

pub(super) fn text_response(status: u16, msg: &'static str) -> Response<ProxyBody> {
    let body = Full::new(Bytes::from(msg))
        .map_err(|_: Infallible| unreachable!())
        .boxed();
    Response::builder().status(status).body(body).expect("valid response")
}

pub(super) fn json_response(status: u16, body: Vec<u8>) -> Response<ProxyBody> {
    let body = Full::new(Bytes::from(body))
        .map_err(|_: Infallible| unreachable!())
        .boxed();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(body)
        .expect("valid response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn text_response_status_code() {
        let resp = text_response(400, "bad request");
        assert_eq!(resp.status().as_u16(), 400);
    }

    #[tokio::test]
    async fn text_response_body_content() {
        let resp = text_response(200, "hello");
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn json_response_status_code() {
        let resp = json_response(201, b"{}".to_vec());
        assert_eq!(resp.status().as_u16(), 201);
    }

    #[tokio::test]
    async fn json_response_content_type_header() {
        let resp = json_response(200, b"{}".to_vec());
        let ct = resp.headers().get("content-type").unwrap();
        assert_eq!(ct, "application/json");
    }

    #[tokio::test]
    async fn json_response_body_content() {
        let body_data = br#"{"key":"value"}"#.to_vec();
        let resp = json_response(200, body_data.clone());
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), body_data.as_slice());
    }
}
