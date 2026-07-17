use super::{
    OptionInstrument, OptionMarketPoint, OptionRight, OptionsProvider, OptionsUnderlying,
    RawOptionChainSnapshot, RawOptionContractSnapshot,
};
use crate::{UnixMs, adapter};
use reqwest::Client;
use serde::Deserialize;
use std::{collections::HashMap, time::Duration};
use thiserror::Error;

const PRODUCTION_BASE_URL: &str = "https://www.deribit.com/api/v2";
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum DeribitError {
    #[error("failed to build Deribit HTTP client: {0}")]
    Client(#[source] reqwest::Error),
    #[error("Deribit HTTP request failed: {0}")]
    Request(#[source] reqwest::Error),
    #[error("Deribit returned HTTP {status}: {message}")]
    Http { status: u16, message: String },
    #[error("invalid Deribit JSON response: {0}")]
    Decode(#[source] serde_json::Error),
    #[error("Deribit JSON-RPC error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("Deribit JSON-RPC response did not contain result")]
    MissingResult,
    #[error("Deribit returned no valid option contracts")]
    EmptySnapshot,
}

#[derive(Debug, Clone)]
pub struct DeribitOptionsClient {
    client: Client,
    base_url: String,
}

impl DeribitOptionsClient {
    pub fn new(proxy: Option<&adapter::Proxy>) -> Result<Self, DeribitError> {
        Self::with_base_url(PRODUCTION_BASE_URL, proxy)
    }

    pub fn with_base_url(
        base_url: impl Into<String>,
        proxy: Option<&adapter::Proxy>,
    ) -> Result<Self, DeribitError> {
        let builder = Client::builder()
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .timeout(HTTP_REQUEST_TIMEOUT);
        let client = adapter::proxy::try_apply_proxy(builder, proxy)
            .build()
            .map_err(DeribitError::Client)?;
        Ok(Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_owned(),
        })
    }

    pub async fn fetch_instruments(
        &self,
        underlying: OptionsUnderlying,
    ) -> Result<Vec<OptionInstrument>, DeribitError> {
        log::info!("GEX FetchStarted kind=instruments underlying={underlying} provider=Deribit");
        let result = self
            .get::<Vec<InstrumentDto>>(
                "public/get_instruments",
                underlying,
                &[("expired", "false")],
            )
            .await;
        match result {
            Ok(items) => {
                let instruments = items
                    .into_iter()
                    .filter_map(|dto| dto.into_model(underlying))
                    .collect::<Vec<_>>();
                log::info!(
                    "GEX InstrumentsRefreshed underlying={underlying} count={}",
                    instruments.len()
                );
                Ok(instruments)
            }
            Err(error) => {
                log::warn!(
                    "GEX FetchFailed kind=instruments underlying={underlying} error={error}"
                );
                Err(error)
            }
        }
    }

    pub async fn fetch_chain(
        &self,
        underlying: OptionsUnderlying,
        instruments: &[OptionInstrument],
    ) -> Result<RawOptionChainSnapshot, DeribitError> {
        log::info!("GEX FetchStarted kind=snapshot underlying={underlying} provider=Deribit");
        let summaries = self
            .get::<Vec<BookSummaryDto>>("public/get_book_summary_by_currency", underlying, &[])
            .await
            .inspect_err(|error| {
                log::warn!("GEX FetchFailed kind=snapshot underlying={underlying} error={error}");
            })?;
        let snapshot = merge_snapshot(underlying, instruments, summaries, UnixMs::now())?;
        log::info!(
            "GEX SnapshotRefreshed underlying={underlying} contracts={} observed_at={}",
            snapshot.contracts.len(),
            snapshot.observed_at
        );
        log::info!("GEX FetchCompleted kind=snapshot underlying={underlying}");
        Ok(snapshot)
    }

    async fn get<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        underlying: OptionsUnderlying,
        extra: &[(&str, &str)],
    ) -> Result<T, DeribitError> {
        let mut request = self
            .client
            .get(format!("{}/{}", self.base_url, method))
            .query(&[("currency", underlying.as_str()), ("kind", "option")]);
        if !extra.is_empty() {
            request = request.query(extra);
        }
        let response = request.send().await.map_err(DeribitError::Request)?;
        let status = response.status();
        let body = response.text().await.map_err(DeribitError::Request)?;
        if !status.is_success() {
            return Err(DeribitError::Http {
                status: status.as_u16(),
                message: body.chars().take(256).collect(),
            });
        }
        parse_rpc(&body)
    }
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    jsonrpc: Option<String>,
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

fn parse_rpc<T: for<'de> Deserialize<'de>>(body: &str) -> Result<T, DeribitError> {
    let envelope: JsonRpcResponse<T> = serde_json::from_str(body).map_err(DeribitError::Decode)?;
    if envelope.jsonrpc.as_deref() != Some("2.0") {
        return Err(DeribitError::MissingResult);
    }
    if let Some(error) = envelope.error {
        return Err(DeribitError::Rpc {
            code: error.code,
            message: error.message,
        });
    }
    envelope.result.ok_or(DeribitError::MissingResult)
}

#[derive(Debug, Clone, Deserialize)]
struct InstrumentDto {
    instrument_name: String,
    expiration_timestamp: u64,
    strike: f64,
    option_type: String,
    contract_size: f64,
    #[serde(default)]
    is_active: bool,
}

impl InstrumentDto {
    fn into_model(self, underlying: OptionsUnderlying) -> Option<OptionInstrument> {
        let right = match self.option_type.as_str() {
            "call" => OptionRight::Call,
            "put" => OptionRight::Put,
            _ => return None,
        };
        (self.is_active
            && self.strike.is_finite()
            && self.strike > 0.0
            && self.contract_size.is_finite()
            && self.contract_size > 0.0)
            .then_some(OptionInstrument {
                instrument_name: self.instrument_name,
                underlying,
                expiration_timestamp: UnixMs::new(self.expiration_timestamp),
                strike: self.strike,
                right,
                contract_size: self.contract_size,
            })
    }
}

#[derive(Debug, Deserialize)]
struct BookSummaryDto {
    instrument_name: String,
    open_interest: Option<f64>,
    mark_iv: Option<f64>,
    underlying_price: Option<f64>,
    interest_rate: Option<f64>,
    creation_timestamp: Option<u64>,
}

fn merge_snapshot(
    underlying: OptionsUnderlying,
    instruments: &[OptionInstrument],
    summaries: Vec<BookSummaryDto>,
    now: UnixMs,
) -> Result<RawOptionChainSnapshot, DeribitError> {
    let metadata = instruments
        .iter()
        .map(|instrument| (instrument.instrument_name.as_str(), instrument))
        .collect::<HashMap<_, _>>();
    let mut contracts = Vec::new();
    let mut missing_metadata = 0usize;
    let mut invalid = 0usize;

    for summary in summaries {
        let Some(instrument) = metadata.get(summary.instrument_name.as_str()) else {
            missing_metadata += 1;
            continue;
        };
        let Some((oi, iv, spot, rate)) = summary
            .open_interest
            .zip(summary.mark_iv)
            .zip(summary.underlying_price)
            .zip(summary.interest_rate)
            .map(|(((oi, iv), spot), rate)| (oi, iv, spot, rate))
        else {
            invalid += 1;
            continue;
        };
        if instrument.expiration_timestamp <= now
            || ![oi, iv, spot, rate].iter().all(|value| value.is_finite())
            || oi < 0.0
            || iv <= 0.0
            || spot <= 0.0
        {
            invalid += 1;
            continue;
        }
        contracts.push(RawOptionContractSnapshot {
            instrument: (*instrument).clone(),
            market: OptionMarketPoint {
                instrument_name: summary.instrument_name,
                open_interest_underlying: oi,
                mark_iv_percent: iv,
                underlying_price: spot,
                interest_rate: rate,
                observed_at: UnixMs::new(summary.creation_timestamp.unwrap_or(now.as_u64())),
            },
        });
    }
    if missing_metadata > 0 || invalid > 0 {
        log::debug!(
            "GEX SnapshotMerge underlying={underlying} missing_metadata={missing_metadata} invalid={invalid}"
        );
    }
    if contracts.is_empty() {
        return Err(DeribitError::EmptySnapshot);
    }
    let source_spot = contracts
        .iter()
        .map(|contract| contract.market.underlying_price)
        .sum::<f64>()
        / contracts.len() as f64;
    Ok(RawOptionChainSnapshot {
        provider: OptionsProvider::Deribit,
        underlying,
        source_spot,
        contracts: contracts.into(),
        observed_at: now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    const INSTRUMENTS: &str = r#"{"jsonrpc":"2.0","result":[{"instrument_name":"BTC-30JUN30-100000-C","expiration_timestamp":1909008000000,"strike":100000.0,"option_type":"call","contract_size":1.0,"is_active":true}]}"#;
    const SUMMARIES: &str = r#"{"jsonrpc":"2.0","result":[{"instrument_name":"BTC-30JUN30-100000-C","open_interest":12.5,"mark_iv":55.0,"underlying_price":101000.0,"interest_rate":0.01,"creation_timestamp":1800000000000},{"instrument_name":"UNKNOWN","open_interest":1.0,"mark_iv":50.0,"underlying_price":101000.0,"interest_rate":0.0}]}"#;

    #[test]
    fn parses_instruments_and_nullable_summary_fields() {
        let dtos: Vec<InstrumentDto> = parse_rpc(INSTRUMENTS).expect("fixture");
        let model = dtos[0]
            .to_owned()
            .into_model(OptionsUnderlying::Btc)
            .expect("valid");
        assert_eq!(model.right, OptionRight::Call);
        let nullable = r#"{"jsonrpc":"2.0","result":[{"instrument_name":"x","open_interest":null,"mark_iv":null,"underlying_price":null,"interest_rate":null,"creation_timestamp":null}]}"#;
        let values: Vec<BookSummaryDto> = parse_rpc(nullable).expect("nullable");
        assert!(values[0].open_interest.is_none());
    }

    #[test]
    fn validates_json_rpc_error_and_missing_result() {
        let error = parse_rpc::<Vec<InstrumentDto>>(
            r#"{"jsonrpc":"2.0","error":{"code":-1,"message":"bad"}}"#,
        );
        assert!(matches!(error, Err(DeribitError::Rpc { code: -1, .. })));
        assert!(matches!(
            parse_rpc::<Vec<InstrumentDto>>(r#"{"jsonrpc":"2.0"}"#),
            Err(DeribitError::MissingResult)
        ));
    }

    #[test]
    fn merges_by_instrument_name_and_skips_unknown_or_missing() {
        let instruments = parse_rpc::<Vec<InstrumentDto>>(INSTRUMENTS)
            .expect("fixture")
            .into_iter()
            .filter_map(|dto| dto.into_model(OptionsUnderlying::Btc))
            .collect::<Vec<_>>();
        let summaries = parse_rpc::<Vec<BookSummaryDto>>(SUMMARIES).expect("fixture");
        let snapshot = merge_snapshot(
            OptionsUnderlying::Btc,
            &instruments,
            summaries,
            UnixMs::new(1_800_000_000_000),
        )
        .expect("snapshot");
        assert_eq!(snapshot.contracts.len(), 1);
        assert_eq!(snapshot.contracts[0].market.open_interest_underlying, 12.5);
    }

    #[tokio::test]
    async fn injectable_base_url_uses_batch_instruments_endpoint() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("address");
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("connection");
            let mut request = [0u8; 4096];
            let count = stream.read(&mut request).expect("request");
            let request = String::from_utf8_lossy(&request[..count]);
            assert!(request.starts_with("GET /api/v2/public/get_instruments?"));
            assert!(request.contains("currency=BTC"));
            assert!(request.contains("kind=option"));
            assert!(request.contains("expired=false"));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                INSTRUMENTS.len(),
                INSTRUMENTS
            );
            stream.write_all(response.as_bytes()).expect("response");
        });
        let client = DeribitOptionsClient::with_base_url(format!("http://{address}/api/v2"), None)
            .expect("client");
        let instruments = client
            .fetch_instruments(OptionsUnderlying::Btc)
            .await
            .expect("instruments");
        assert_eq!(instruments.len(), 1);
        server.join().expect("server");
    }
}
