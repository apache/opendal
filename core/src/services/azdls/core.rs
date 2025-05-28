// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use base64::Engine;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::IF_NONE_MATCH;
use http::HeaderName;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::StatusCode;
use reqsign::AzureStorageCredential;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

const X_MS_RENAME_SOURCE: &str = "x-ms-rename-source";
const X_MS_VERSION: &str = "x-ms-version";
pub const DIRECTORY: &str = "directory";
pub const FILE: &str = "file";

pub struct AzdlsCore {
    pub info: Arc<AccessorInfo>,
    pub filesystem: String,
    pub root: String,
    pub endpoint: String,

    pub loader: AzureStorageLoader,
    pub signer: AzureStorageSigner,
}

impl Debug for AzdlsCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzdlsCore")
            .field("filesystem", &self.filesystem)
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl AzdlsCore {
    async fn load_credential(&self) -> Result<AzureStorageCredential> {
        let cred = self
            .loader
            .load()
            .await
            .map_err(new_request_credential_error)?;

        if let Some(cred) = cred {
            Ok(cred)
        } else {
            Err(Error::new(
                ErrorKind::ConfigInvalid,
                "no valid credential found",
            ))
        }
    }

    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = self.load_credential().await?;
        // Insert x-ms-version header for normal requests.
        req.headers_mut().insert(
            HeaderName::from_static(X_MS_VERSION),
            // 2022-11-02 is the version supported by Azurite V3 and
            // used by Azure Portal, We use this version to make
            // sure most our developer happy.
            //
            // In the future, we could allow users to configure this value.
            HeaderValue::from_static("2022-11-02"),
        );
        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }
}

impl AzdlsCore {
    pub async fn azdls_read(&self, path: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        let mut req = req
            .extension(Operation::Read)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.info.http_client().fetch(req).await
    }

    /// resource should be one of `file` or `directory`
    ///
    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
    pub async fn azdls_create(
        &self,
        path: &str,
        resource: &str,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?resource={resource}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // Content length must be 0 for create request.
        req = req.header(CONTENT_LENGTH, 0);

        if let Some(ty) = args.content_type() {
            req = req.header(CONTENT_TYPE, ty)
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos)
        }

        if args.if_not_exists() {
            req = req.header(IF_NONE_MATCH, "*")
        }

        if let Some(v) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, v)
        }

        let operation = if resource == DIRECTORY {
            Operation::CreateDir
        } else {
            Operation::Write
        };

        let mut req = req
            .extension(operation)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_rename(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&target)
        );

        let source_path = format!("/{}/{}", self.filesystem, percent_encode_path(&source));

        let mut req = Request::put(&url)
            .header(X_MS_RENAME_SOURCE, source_path)
            .header(CONTENT_LENGTH, 0)
            .extension(Operation::Rename)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    pub async fn azdls_update(
        &self,
        path: &str,
        size: Option<u64>,
        position: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        // - close: Make this is the final action to this file.
        // - flush: Flush the file directly.
        let url = format!(
            "{}/{}/{}?action=append&close=true&flush=true&position={}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p),
            position
        );

        let mut req = Request::patch(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        let mut req = req
            .extension(Operation::Write)
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_get_properties(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?action=getStatus",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let mut req = req
            .extension(Operation::Stat)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_stat_metadata(&self, path: &str) -> Result<Metadata> {
        let resp = self.azdls_get_properties(path).await?;

        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let meta = parse_into_metadata(path, resp.headers())?;
        let resource = resp
            .headers()
            .get("x-ms-resource-type")
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "azdls should return x-ms-resource-type header, but it's missing",
                )
            })?
            .to_str()
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "azdls should return x-ms-resource-type header, but it's not a valid string",
                )
                .set_source(err)
            })?;

        match resource {
            FILE => Ok(meta.with_mode(EntryMode::FILE)),
            DIRECTORY => Ok(meta.with_mode(EntryMode::DIR)),
            v => Err(Error::new(
                ErrorKind::Unexpected,
                "azdls returns an unknown x-ms-resource-type",
            )
            .with_context("resource", v)),
        }
    }

    pub async fn azdls_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::delete(&url)
            .extension(Operation::Delete)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_list(
        &self,
        path: &str,
        start_after: Option<&str>,
        continuation: &str,
        limit: Option<usize>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let mut url = QueryPairsWriter::new(&format!("{}/{}", self.endpoint, self.filesystem))
            .push("resource", "filesystem")
            .push("recursive", "false");
        if !p.is_empty() {
            url = url.push("directory", &percent_encode_path(&p));
        }
        if let Some(limit) = limit {
            url = url.push("maxResults", &limit.to_string());
        }

        let continuation_value = if !continuation.is_empty() {
            Some(percent_encode_path(continuation))
        } else if let Some(start_after) = start_after {
            Some(generate_continuation_from_start_after(start_after))
        } else {
            None
        };
        if let Some(c) = continuation_value {
            url = url.push("continuation", &c);
        }

        let mut req = Request::get(url.finish())
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn azdls_ensure_parent_path(&self, path: &str) -> Result<Option<Response<Buffer>>> {
        let abs_target_path = path.trim_end_matches('/').to_string();
        let abs_target_path = abs_target_path.as_str();
        let mut parts: Vec<&str> = abs_target_path
            .split('/')
            .filter(|x| !x.is_empty())
            .collect();

        if !parts.is_empty() {
            parts.pop();
        }

        if !parts.is_empty() {
            let parent_path = parts.join("/");
            let resp = self
                .azdls_create(&parent_path, DIRECTORY, &OpWrite::default())
                .await?;

            Ok(Some(resp))
        } else {
            Ok(None)
        }
    }
}

// See this workaround implemenation in the azure hadoop implementation:
// https://github.com/apache/hadoop/blob/66adc68b62058a63598172cb14bf514287f6584a/hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azurebfs/AzureBlobFileSystemStore.java#L1306
fn generate_continuation_from_start_after(start_after: &str) -> String {
    let crc = compute_crc64(format!("{}#$0", start_after).as_bytes());
    let token = format!("{} 0 {}", crc, start_after);
    let token = base64::prelude::BASE64_STANDARD.encode(token.as_bytes());
    token
}

fn compute_crc64(input: &[u8]) -> i64 {
    let mut value = -1i64;

    for &byte in input {
        let idx = (((byte as i32) ^ (value as i32)) & 0xFF) as usize;
        value = TABLE[idx] ^ ((value as u64) >> 8) as i64;
    }
    !value
}

// Generated with poly 0x9a6c9329ac4bc9b5
const TABLE: [i64; 256] = [
    0,
    9182541432847960441,
    -81661208013630734,
    -9100911350982468725,
    -3935330839729949041,
    -5328860363356880906,
    4016934769805403261,
    5247243509741595908,
    -6477041904481141131,
    -2778126699754064116,
    6395407394255400071,
    2859783479402063358,
    8033869539610806522,
    1157698950281609603,
    -7952257054226359800,
    -1239307248593022095,
    8710242310496874369,
    544390144406054648,
    -8773822775353311885,
    -480778622590716918,
    -5655929285198751474,
    -3536193771365838729,
    5719566958804126716,
    3472568952111055493,
    -2379004994487938572,
    -6804126189421127539,
    2315397900563219206,
    6867711082173303423,
    1702069273413494651,
    7561550595985681922,
    -1638440086397766263,
    -7625183901824729872,
    -1026259452715802878,
    -8165291385339423109,
    1088780288812109296,
    8102801665828209801,
    4081135393624123789,
    5174050811428790516,
    -4143599089657414785,
    -5111574183165038074,
    6331237281917575543,
    2932936320451717134,
    -6268743143072255099,
    -2995452737208534276,
    -7007610156101298184,
    -2174948929557487999,
    6945137904222110986,
    2237417001980464243,
    -8565563587773157245,
    -698073865129608710,
    8646032624330580593,
    617573780371024648,
    4630795801126438412,
    4552317850264964981,
    -4711321909362944770,
    -4471804605874987641,
    3404138546826989302,
    5788002041349785487,
    -3323642881738187772,
    -5868475497582111363,
    -1846747927333570439,
    -7407866943897440000,
    1766230306223614603,
    7488388675408585714,
    -2928788100313371281,
    -6326384893301644266,
    2992425542307102621,
    6262760941951170276,
    2177560577624218592,
    7014021097877803673,
    -2241140742053132014,
    -6950410375142506389,
    8162270787248247578,
    1020283848406030947,
    -8098642450851970584,
    -1083916905357227887,
    -5177781148310608491,
    -4086414461352612628,
    5114174836390786919,
    4149999036593995294,
    -5784269509874400530,
    -3398857284503876713,
    5865872640903434268,
    3317240731349735781,
    7410885347125621857,
    1852721336781405464,
    -7492545687201323373,
    -1771091486493937686,
    695464411657452699,
    8559154840590169570,
    -613852243750310295,
    -8640762356863195376,
    -4556468265265329644,
    -4635650384605445267,
    4474834003960928486,
    4717306313667482015,
    2781857646629810797,
    6482320345254034196,
    -2862383522617106273,
    -6401807968130799130,
    -1154678825048390430,
    -8027893446085259877,
    1235147560742049296,
    7947394159970475881,
    -9185152471456674792,
    -6411569270127263,
    9104635700529929962,
    86933051457181587,
    5324711670898473623,
    3930478940865573870,
    -5244216787177002907,
    -4010952078949539556,
    6808277093653978604,
    2383859105125700757,
    -6870739991009980642,
    -2321382777692223897,
    -7558941769418855581,
    -1695659916186312166,
    7621461738281908625,
    1633170428957798632,
    -547408057865837671,
    -8716216192247928096,
    484936124168630635,
    8778683483337193490,
    3532460612447229206,
    5650648632384052335,
    -3469966722892380188,
    -5713164198924038499,
    -7319313487190308427,
    -1944882268426321716,
    7255706616989801287,
    2008467384902701630,
    5984851084614205242,
    3197703697127700035,
    -5921222189807211064,
    -3261337295175490383,
    4355121155248437184,
    4836460649178119865,
    -4418701877953944270,
    -4772849385211843509,
    -787396636298018481,
    -8467776486107314122,
    851034636706747325,
    8404151993655892676,
    -2122202499213056460,
    -7069916004553314483,
    2040567696812061894,
    7151572492026068415,
    3165618640958787771,
    6089000465318648258,
    -3084005931816648119,
    -6170608539872359632,
    4940590242968197185,
    4323016312165290296,
    -5022251124144846157,
    -4241385903462825014,
    -8218394400927977778,
    -964732264542541897,
    8299998073187990588,
    883115153111807301,
    7758977986698090167,
    1496212771153551310,
    -7678482544561748923,
    -1576686450338329284,
    -6714998791902683080,
    -2476565394777495231,
    6634481462699471562,
    2557087418195393459,
    -3624973379458307902,
    -5557599020465626693,
    3705442673562810928,
    5477099193254114121,
    347732205828726349,
    8916445914979620660,
    -428258640096834369,
    -8835932996621253178,
    1390928823314905398,
    7792180275546222671,
    -1328434392529212476,
    -7854696400362675523,
    -2724828310247256135,
    -6538795793762138432,
    2662355835449236811,
    6601263643266274354,
    -5381380642047162557,
    -3873220914650280390,
    5443901152145348017,
    3810730869140954312,
    8949668007921856972,
    242468062084315317,
    -9012131446374587586,
    -179991176239994297,
    5563715293259621594,
    3627853308494900643,
    -5482103383201483224,
    -3709461082616922287,
    -8911307653206748587,
    -343861062487909588,
    8829673718705766567,
    425517318353373662,
    -1501205965726213457,
    -7762985400376454186,
    1582809320479393885,
    7681369070946451748,
    2470295121484098592,
    6712246474782352729,
    -2551955753768599854,
    -6630616916703692885,
    3879488993349045083,
    5384130764573901346,
    -3815860330115851863,
    -5447763494688190256,
    -237472672649691692,
    -8945658399380491091,
    173866102914363174,
    9009242716806358623,
    -7797320731912604370,
    -1394802161250272169,
    7860957881731147740,
    1331177917724618405,
    6532681715831194529,
    2721950576072674008,
    -6596261656502349485,
    -2658339629579610070,
    -4830189886401594408,
    -4352369310716611935,
    4767718210251401514,
    4414836859352883283,
    8472769053991669079,
    791404660239696942,
    -8410275490468487259,
    -853920552811358500,
    1939744496674522541,
    7315441871796849876,
    -2002207616982919329,
    -7252965767315759578,
    -3203820597145734366,
    -5987730403925296549,
    3266340857915597264,
    5925241208603601065,
    -4316901623920634791,
    -4937713135431525088,
    4236384526995834539,
    5018234291620532178,
    969872248337261270,
    8222268228363316143,
    -889377107035164636,
    -8302741108866647715,
    7064921224894458412,
    2118192263497917269,
    -7145446808941446946,
    -2037679594434383449,
    -6095269016053343069,
    -3168368273447741990,
    6175737528828104273,
    3087868764414052136,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc64() {
        let input = "helloworld.pdf".as_bytes();
        let expected = -8247990622076416313;
        assert_eq!(compute_crc64(input), expected);
    }
}
