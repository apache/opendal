"use strict";(self.webpackChunkopendal_website=self.webpackChunkopendal_website||[]).push([[277],{6226:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>u,contentTitle:()=>p,default:()=>j,frontMatter:()=>f,metadata:()=>s,toc:()=>x});const s=JSON.parse('{"type":"mdx","permalink":"/download","source":"@site/src/pages/download.mdx","title":"Apache OpenDAL\u2122 Downloads","description":"The official Apache OpenDAL releases are provided as source artifacts.","frontMatter":{},"unlisted":false}');var n=a(6070),r=a(5658),i=(a(758),a(5449)),o=a(6381);function c(){const{siteConfig:{customFields:e}}=(0,i.A)(),t=e.version,a=`https://www.apache.org/dyn/closer.lua/opendal/${t}/apache-opendal-core-${t}-src.tar.gz?action=download`;return(0,n.jsx)(o.A,{to:a,children:t})}function l(){const{siteConfig:{customFields:e}}=(0,i.A)(),t=e.version,a=`https://downloads.apache.org/opendal/${t}/apache-opendal-core-${t}-src.tar.gz.asc`;return(0,n.jsx)(o.A,{to:a,children:"asc"})}function h(){const{siteConfig:{customFields:e}}=(0,i.A)(),t=e.version,a=`https://downloads.apache.org/opendal/${t}/apache-opendal-core-${t}-src.tar.gz.sha512`;return(0,n.jsx)(o.A,{to:a,children:"sha512"})}function d(){return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(c,{})," (",(0,n.jsx)(l,{}),", ",(0,n.jsx)(h,{}),")"]})}const f={},p="Apache OpenDAL\u2122 Downloads",u={},x=[{value:"Releases",id:"releases",level:2},{value:"Notes",id:"notes",level:2},{value:"To verify the signature of the release artifact",id:"to-verify-the-signature-of-the-release-artifact",level:3},{value:"To verify the checksum of the release artifact",id:"to-verify-the-checksum-of-the-release-artifact",level:3}];function g(e){const t={a:"a",code:"code",h1:"h1",h2:"h2",h3:"h3",header:"header",li:"li",p:"p",pre:"pre",ul:"ul",...(0,r.R)(),...e.components};return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(t.header,{children:(0,n.jsx)(t.h1,{id:"apache-opendal-downloads",children:"Apache OpenDAL\u2122 Downloads"})}),"\n",(0,n.jsx)(t.p,{children:"The official Apache OpenDAL releases are provided as source artifacts."}),"\n",(0,n.jsx)(t.h2,{id:"releases",children:"Releases"}),"\n","\n",(0,n.jsxs)(t.p,{children:["The latest source release is ",(0,n.jsx)(d,{}),"."]}),"\n",(0,n.jsxs)(t.p,{children:["For older releases, please check the ",(0,n.jsx)(t.a,{href:"https://archive.apache.org/dist/opendal/",children:"archive"}),"."]}),"\n",(0,n.jsxs)(t.p,{children:["For even older releases during the incubating phase, please check the ",(0,n.jsx)(t.a,{href:"https://archive.apache.org/dist/incubator/opendal/",children:"incubator archive"}),"."]}),"\n",(0,n.jsx)(t.h2,{id:"notes",children:"Notes"}),"\n",(0,n.jsxs)(t.ul,{children:["\n",(0,n.jsx)(t.li,{children:"When downloading a release, please verify the OpenPGP compatible signature (or failing that, check the SHA-512); these should be fetched from the main Apache site."}),"\n",(0,n.jsx)(t.li,{children:"The KEYS file contains the public keys used for signing release. It is recommended that (when possible) a web of trust is used to confirm the identity of these keys."}),"\n",(0,n.jsxs)(t.li,{children:["Please download the ",(0,n.jsx)(t.a,{href:"https://downloads.apache.org/opendal/KEYS",children:"KEYS"})," as well as the .asc signature files."]}),"\n"]}),"\n",(0,n.jsx)(t.h3,{id:"to-verify-the-signature-of-the-release-artifact",children:"To verify the signature of the release artifact"}),"\n",(0,n.jsx)(t.p,{children:"You will need to download both the release artifact and the .asc signature file for that artifact. Then verify the signature by:"}),"\n",(0,n.jsxs)(t.ul,{children:["\n",(0,n.jsx)(t.li,{children:"Download the KEYS file and the .asc signature files for the relevant release artifacts."}),"\n",(0,n.jsx)(t.li,{children:"Import the KEYS file to your GPG keyring:"}),"\n"]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-shell",children:"gpg --import KEYS\n"})}),"\n",(0,n.jsxs)(t.ul,{children:["\n",(0,n.jsx)(t.li,{children:"Verify the signature of the release artifact using the following command:"}),"\n"]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-shell",children:"gpg --verify <artifact>.asc <artifact>\n"})}),"\n",(0,n.jsx)(t.h3,{id:"to-verify-the-checksum-of-the-release-artifact",children:"To verify the checksum of the release artifact"}),"\n",(0,n.jsx)(t.p,{children:"You will need to download both the release artifact and the .sha512 checksum file for that artifact. Then verify the checksum by:"}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-shell",children:"shasum -a 512 -c <artifact>.sha512\n"})})]})}function j(e={}){const{wrapper:t}={...(0,r.R)(),...e.components};return t?(0,n.jsx)(t,{...e,children:(0,n.jsx)(g,{...e})}):g(e)}},5658:(e,t,a)=>{a.d(t,{R:()=>i,x:()=>o});var s=a(758);const n={},r=s.createContext(n);function i(e){const t=s.useContext(r);return s.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function o(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(n):e.components||n:i(e.components),s.createElement(r.Provider,{value:t},e.children)}}}]);