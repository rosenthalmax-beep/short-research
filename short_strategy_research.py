"""EURCHF M15 SHORT Pass3: bounded compression/downside-breakout discovery.
Python3.10+ standard library; no broker calls or order capability.
Use SAME EURCHF_M15_PASS1_FROZEN_DATA.zip alongside this single Python file.
27 new settings +3 unrestricted-breakdown references +2 archived anchors.
RR3, costs1/2/4pip, full2005-Oct8 history; /status, /results; --run/--self-test.
"""
from __future__ import annotations
import argparse,base64,bisect,csv,hashlib,io,json,math,os,shutil,sys,tempfile,threading,time,traceback,zipfile,zlib
from collections import defaultdict,deque
from datetime import datetime,timedelta,timezone
from pathlib import Path
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIServer,make_server
VERSION='EURCHF_M15_SHORT_PASS3_COMPRESSION_DISCOVERY_V1_2026_10_08'
PAIR,SIDE,TIMEFRAME='EUR_CHF','SELL','M15'
UTC=timezone.utc
START=datetime(2005,1,1,tzinfo=UTC)
END=datetime(2026,10,8,tzinfo=UTC)
BAR=timedelta(minutes=15)
H1_BAR=timedelta(hours=1)
PINNED_H1_END=datetime(2026,10,1,tzinfo=UTC)
PINNED_H1_COUNT=137819
PINNED_H1_SHA256='9c8b5279629daee868ad924f52e998be7f6ce638a15e106ffb4851a2df76feac'
TICK,PIP,RR,WARMUP=.00001,.0001,3.,200
COSTS=(10,20,40)
MODELS=('NEAREST_OPEN_SENSITIVITY','STOP_FIRST_GAP_STRESS')
BOX_WINDOWS=(8,12,16)
WIDTH_CAPS=(1.5,2.,2.5)
BODY_FLOORS=(0.,.25,.5)
PARENT_IDS=('M_L200_D010_B125_R000','B_STRUCTURE_L200_D0075_B125')
COMPARISON_IDS=PARENT_IDS+tuple(f'BREAKDOWN_ONLY_N{n:02d}' for n in BOX_WINDOWS)
RESULT_NAME='EURCHF_M15_SHORT_PASS3_COMPRESSION_DISCOVERY_RESULTS.zip'
OUT=Path(os.getenv('EURCHF_M15_PASS3_OUTPUT_DIR','/tmp/eurchf_m15_short_pass3')).resolve()
PREFIX='/eurchf-m15-short-pass3'
LOCK=threading.RLock()
STARTED=False
JOB_LOCK=None
RUN_CLOCK=None
STATUS=dict(state='not_started',progress=0,message='Ready',version=VERSION,orders_supported=False,trading_enabled=False,pair=PAIR)
CASE=['config_id', 'execution_model', 'cost_ticks']
SUMMARY_FIELDS=['config_id', 'execution_model', 'cost_ticks', 'raw_signals', 'eligible_isolated_signals', 'isolated_completed', 'isolated_open', 'isolated_total_r', 'isolated_profit_factor', 'geometry_invalid_all_signals', 'p0_blocked_signals', 'invalid_unblocked_entries', 'open_at_data_end', 'closed_trades', 'wins', 'losses', 'win_rate_pct', 'total_r', 'expectancy_r', 'profit_factor', 'max_closed_dd_r', 'max_losing_streak', 'dual_touch_closed', 'gap_stop_closed', 'gap_target_closed', 'entry_next_open_gap_count', 'entries_before_market_closure', 'median_stop_pips', 'median_cost_fraction_reference_risk', 'p90_cost_fraction_reference_risk', 'median_holding_hours', 'max_holding_hours', 'maximum_inter_entry_gap_days', 'leading_no_entry_days', 'trailing_no_entry_days', 'zero_entry_months', 'max_consecutive_zero_entry_months', 'positive_complete_entry_years', 'negative_complete_entry_years', 'zero_entry_complete_years', 'raw_signal_sha256', 'accepted_ledger_sha256', 'worst_12m_realized_r', 'worst_12m_start', 'worst_12m_end', 'zero_entry_12m_windows', 'worst_24m_realized_r', 'worst_24m_start', 'worst_24m_end', 'zero_entry_24m_windows', 'worst_36m_realized_r', 'worst_36m_start', 'worst_36m_end', 'zero_entry_36m_windows']
EMBEDDED_MEMBER_PINS={'source_candles.csv': {'bytes': 31136287, 'sha256': '59f3a83fb838d9921f9f2433cd928a77ef451301bec466a3fedd67744ae2f707'}, 'source_h1_crosscheck_candles.csv': {'bytes': 7942356, 'sha256': 'ffaa24aa4c371b7885c61ef225f1846ccf5e133d444a45c438af3066cef702a3'}, 'fetch_receipts.csv': {'bytes': 19964, 'sha256': '52423125b321d3c25b5ae2adf89c8fd287b230764540847b375279cc92530890'}, 'parent_pass1_accepted_ledgers.csv': {'bytes': 23899883, 'sha256': '05bef6ac72de3d5d7c4a8e6ca0fc2dc52c13c684f956e9f3c88cf8f4aea4a706'}, 'parent_pass1_provenance.json': {'bytes': 71790, 'sha256': 'f11996dfcd59473762a35b3cd02ab627a3b9757d71f2021b593999f5ce60ea1c'}}
EMBEDDED_PAYLOAD_SHA256='511215ae3b95e676829896d30baa1342bbca17e7be30745a22753dab5ba3b849'
FROZEN_SOURCE_FILE=Path(__file__).resolve().with_name('EURCHF_M15_PASS1_FROZEN_DATA.zip')
ARCHIVE_REFERENCE_SHA256='1438252a83cae9a7c96718d27c1591fe6d083f5a64a4d4b589b2374e722c7863'
ARCHIVE_REFERENCE_B85='c-ri}Ym;2bktOzD_;~>fcaP_Mo0Yg@G?Qk>$X=OAG8wE;l|{nX-Dq`#JHuVme^<nfdo$y4JOc!q%u5p8GawmW6#(*-Kg0d_|NO;IfB5dZryoCDe)`2f{onud7tbHRfBo>=cOPDU@lU__-{1XjY|Oj=)sT7jpNZYOUmIh7@t=Ni`H#!<A3lBf`2D-@KE7PO&0qPizx?N4|MEBAy!)^J_RD{L_nTk->u-Mb&9DBCUw!j`<}ZH!_|vC%pFTYQKHvWfD&PL&)4#v_>BDcofBLo_QMvNl55N8J{@csD4?lhU_UY5*W%`P%;Q8ZsKYV-1|7{k1{NeI_J~i#xr;neWzJ2#&`C|W;f=^^U<^MFH{i+{+{P_Ce)4SKF=T9HEuYJp6t&K)PbmWb7`Ky0>`S{)C(~p0A_u>2h{`BpKmv>L!e%t&x%WwOGdH4R?kI%pF{{rQA+P~!w-yibJ_n+#gzI|i)qrZFl^zP;9(^D3`?8ndFe*CHYC4PK*xzx|1oc{ZV@5}#?a&i8GpEi+CRr2n~{5N{{!}F)@E8do`Z$1Ng>!L}{8Or}j#)b{gA1=?Ip1yzn<MuV?Ey?^v^Cv{fM|M{7m*r0=asHz3p8n(A{zra!*)DrSHvf*1#c{Ivb9Z6?viwyazW?pJpFaI~dHQ|1ul!|R{*eEipFaNK`FHy@`^WIxryufHe*9rR{q*$XZ!e$v^YY35@!j{A|M;~1M7%3s_x$k>-<MA$+WiaiNB{2q<@Mu_mv{Nc|M&YHfB5mT1NPnJ<-^nW^>_W@!w=hsPj79y{S&RdeckqB{`zD7bc$a5czM12c=`VM^6tkEKmC6DOic{=;|vmQiXj;*<ugrY{f*>_lM&{Og^&41@%Eek?&G&F<wN}4$3Og7{($yv*3$j+9c2{g?JoQ9-5<WoKf|9cKfbFURr!Ekp8mM~Oga0OW4-3v%hPtn_aFDK+rNgq&zQ>XpYlh_KcXMLMPJS7tqm!E&+$*pU;RHWKOSH9-N)}g{jUBA<MAW(@%x`P6Z79M@8DM!fPTn7K-*Uw24VlIKVF{dU%320et-Gx)4eZB{h#IfRfoK{SMPrM-4i>X&tDT6Z=$5<L=u@Sy$|7qUtV9W=jgpnjtoC%kyt~Kz4(iNxwvOa_BE?RzV_+)`SL@tJ>Onlew(4V-tB`4FE36mlJ(`8+*7(dUxIseuim|nY%lMPd!Kx|SR0<)Yow=V=bkS9^5UN_PriIM|NimE{8L5zUH*Z}X6Jug4rMf(J-CT~%KB5(f(^?X6W$p5#+ZNF{_p?lzOb64{ACt=S@Q{&ugym2<>S8}A78zL?f7R&Z^cQ<ACNIID+R~yYx_SK;XhdU!s{Q*fiG+RVC8GkA58r8A@OXnZeQE{!SZkC@gJ;wVf%^sJAbh9we275|NBpWX)nx=^upn-a0ou6kd?wZqWf+*s~M%_vO~&vMEU#h!tQPUTZL@a*@tW{tVd4RxcQ6b&&LGGpAD6dC&oxoal-j`%*Msi_Nz%I{)2j93f2hO90sP4{b>xJ>xFm!n}1+ENnrZwgg?&-(`Pv0Eh{*i7Z!SX;%5_|{PXLJ*dUHL-}|1!>uY$qytmKRy}W;Q`C5KT`F{ltPcM$2p5A+Y@lSs<Uf4ThP5mY7m7K;43ou!KS8muo(ha}yZ*7jeEa%vZg|h>>=ZDSPlyeU<#c3zz82sOdBYqR#QcOWOoA%Asrr8s}DY;wsRRTP*d7Hhjm~0FTrTi1?wRAYWO*z;V_Axr9?Kk{Kbi`uwhsimx%fVfCslk4(Bd#vEX2ii-{_2Q7&k>u?al~}SbolwbJlhuvuP-mc>GEV>l1qkyynhK7`}ATh`DZ8MpPYS7A~{EXFY)=}U;Wd2dHEZ0#QD!cR6_XrtFh@sp4ex5<s|1L%y#?v{+n>bzxn3B{@c6%@~dBGU;Hos{BQ4m^G$Zkzxi5{{#ug$T9W=+lKxtf{u6w#c}h>lJ%?mnbn@(<Ej_=;YqT!Cl=S!M<>lF5p0mV0J->g6^z2{X$LE)eL@EFAwIuxyT9W=O7yPvz{k0zbwI2Po9{sf*ea8j6SISwhvCoc8dbzxau;mL|DwVlF)?9oD?%DIpi_Q6j%|99Z6wSrFn&%hy8ot(}|55AFpXGzU7NfrwqrVoTzZRpv7NhTa;rROA@vD1Zs<c-8b8!e+(c*iymz?oFy*}qJxX9D<D~I>__b}o4$;YSgYWclfQu<nq{`)FM|L5XP|J(oa&42s#FAslgg4-PO&)p~;SKc_P&N%)lCFxL#Ae?OUoR!FJhV)KCoZYp!AQ_CTb2(%?hcAweD`wxiVrPLX=FR>#AA)@wT*+kF=N~hPTcSVLq_daFMl*+k`3KU*?PgH5FTPq*EJ0&1<P}#;pI4Jke-%&c!)JJ6qk7_;r(FNxKd~g8^Wl^~Oq2859N_VvP?C1ehVp~!oZs+LJh5bG|2#*WL#4O2b({}y$caAJA^Xp9$nqHuIewu(Htsoje{t@TF8oR%J4XM)&pAnbeR^{C{iVEonfzm)Q(W`P&SvDzdvffu8-CtidQ^YR>Eg3{;{0vT{QkYCr<^?G?@jSpUdlhOK0T%PHhXHmkjvM_d{w$gFfUKS_j;w{U)LSGJMK7~wk~Zxzb+j<&l`smdSiZ=H%@O$V*^okBHrhpbGN8YhvJCD`9#UfSrX$v(;0`i>_ax{ImHSw2A5!UI(uPjt>ai)HIu{7!(v?y+e7{}`e?M`G=FAs`tRHqQ~roTGO4s0<kZO8&#z4jWqa;jOhmG|$FIKl^L_DO@{#y2>5JvrK2eCzHoasE^dy(}FJ4lLj$dr{$u?Y~3;sF1K1ciXY{g_ZY>Zs8!g}K`_HV@(_XTQu8edF*LV@~k*csc0Ipa56YK+EMp-3saWm%M|o#YILY))mI50d^ayzv|RmR<D8a{l=bR%+39u2Q?~1;mG#LsrWv!YXy*>>qOMNI5?iznNp)t|cY5B%4FuV8XSU{SW4kOCad-4<2XTmKf9L7pXl{O!+g6T#6B2-SMB~j{l;y>c6BvPTuD4_9p+_q|4>?)s*h7Y`QF8@^?mxFU0<RczKER^x~}Wg^Bs%<fZglTJyKzk7Hk{b|-SjeW9BFw%qZTHcx*oQ-3W}e=SpgEmMCjQ#Y>Iz7Ma@-bzR@JbB0F^%CjwWbBK7$qDd#_ry=uxXUwT!P86rZ<qM|{PO%FhO&u!|Fum0k6fny(kAM!HR`W5>aR8GuQlqgHR@acR>nfY`+Q9q6E1C(7kPSlk(bNm`SrzLaxk4<-Fvq0J(Zm8IlJWdIl-VD@4bIAFJEia|Ijt+FKwm%TA=<~p#EB*{#u~^TA=Qlx4exnISa7#{=Fm5uTS>%NiHw$HC_CxGtc?>^(DCIUeZ(kjbGotk49cCUtYxer{}K)>c8m%_5YK9;<o?s*P)%h{uusr`G5aV4AAbi_MiCtC%gQ|rYV8y@0MLbd+4rTyZv>IE1l!yzajZ2>-N9y_BS2G_V#I%I|r%zCpy0QW*LubvPF|^X5e=pvXTDr!*l*~zJB=j+e1_=OtIeef9H6r*r&}~>|fzqV_vLRvAH=A_^|!$vlGmpaXU2=Vmkin#}{2M&Obdref)k8g3ABieu&=wHJH*={21VuGjKcM$8jT98Stw=u0g}Eilc(xCMCbXFGpA!ei@z)Kj#McY>Ewh4#Fqh(9<?JH^Dbwb3pmcKmYn)e)-J~Ccn)Q3`ckU)jkqhnW5$EUngkUMw+1|e#7P)qp9XDR#bDBrrS4TXjxQfw-<p1+HD-L$F@~XLD-tuV4J@hsb;<SjH+n?e$m7kzewH=`1v=6-x&7|H=d-yuMuCB?}lHM`p-~Jr<zvhdeiCx5BNDYKZkDCi_eIk1NfyHB=q<ti2>oqTp$n)zbdv0ek`4KLi{{g4ZoWC-thBV9@*g+LW7RS__YvY*H|w;BYu9yPx$!V8vM#3z)#)?eY5Eg#P1Me{j1Wjz%R%0P6IHStpamHFw*26L(cAR5QZ;>aKHJC7zTi0jO-^2W3&Ln9HE2~-Y6IzVvOM?CB7*z#4s4u2L{voz%aT%A9?8lTP00IPzC2}l<>wq63aMaX#+_ZvCM}6OUgNz^s#7*<y8!^tWq+IcqW;k>SHR!<m*4w0h?ykj8WVo@%WtRCY@I`(+sC86XheE&eosW7S6CmD5=-I8=Q?;=J0frlI_wK1gB?bH8`_OI`ir|*a1UN$-=OP+l(>Q68-?(aorh#+XN$Wu9ArK$Q6>=89B-6!8gXi#NsOUD6u%C*{I$POg+V<B1bXy$WihWay~YW(P8atY$k~S?L*gShPAsa`iJ5RYC8F^!c*c=4*iU8Wdp#0Ya<4M>s1;S_}XMOubE}ig|6A`@=(_CwgUzRKIKKc34D;_JZ?Uvl7hlc;TfHx*K#<57~hl}ek;r&5F0T7@hV*u=Db+7GM8mrq<I5Td}<?^0Z&ebd*=#_ZtE_y`9?-}zDCj4oQHd2YYipJzl4WE4GKMp#Z3B|4<pS`$Kni?M6(fNsI$}+dwe%h@l0O#q%}loJgIE$Wt}~CF)ae!Le$g*y3nbDYgBwGUH_jR8uk*X10@^x_w^Ru-6{_7rZ<#|su~RMAtnfK+N%ur;mx0=R!H#W3ABSZ=d5uEc5`gZi(przsRhLpJqmW`YczOsb}Xsz02WScc`Ux5PLyIT3HA8K{k@{YH>W0K`Ztmm->VqnyGvJfhSE`|<uy6<>FW$7=o#=evURe~pxQ2l0VetSttyZ?XYjGK_}XIAABiKV7g6csrRgoDoX(m`yxCF<1LLdMp}@G2-mf-fTc(y9B3aQg)aW|*Io6R(YywyqPKkF-z*Uo*sg5-LCZ~+70>U|v=Nacf!3;R3Er;=?)PNjrBL)ulkS;3`g*9A#^^80`dy<UBT~0Z+xgStTS6T{$Eb@NyDPfH0<=Q1L^V7A@f7Kbru)T5Nf>I+U2xFE~=XN2COscjr^~FJEH=124gvg7>>)x$;^uhwJ(Oy3d*%Dd@$sWV<W3$+V@b$rg)`Wb@2wyQ>sbR#oia~|xDwTy}6~nrk8tT<l$d7^3Hj1}+L+eOAv&RJp?>C>aKLHe*NU{x^!ryY5h87btZ;UNZVHn0%Ji~aCy8AHN(gmr&m}OfwbA{1P;jiU@!y{&HuyePU!y9z_ZKp&IwFZ@@#yBIF)3Pul=eH`Y*N{8J7`ZBq_x+?Kt3H8dndveYNuCGja+DsLv{OORw#B^!6cAmb1-#sJm}Fb~P=rzQkahJ_>JiST0uav0XDH9qAQEcDb0l<=E^-$U-G^qG=}LvAL3Maz%jpL<H3P;@w2g!WInm?hQ!1(;7(K-~60)AfBN*A1UJ~mR7>k%d;<8Ia_fi94WWU)H2qSwUC2=WZ!b34BJG(Z|!P$y5*-A74#J0!Hr$iK#jHu)U(-TcObt9ssJ9c}7VTkBeEC|u7G~QQC_AY2nGs}GEG`Tb?^f2bo$J0tNV_Opx28wyyd`cJ{g3;#idWJEdItXLxgPRe?O$_YoRk}z~vKAywF$=dur;WU`%C|5^D~t;UZRZJt*yp(Il+dAKkc%PCt}&mcFheILLB!c*D<%ltCM9#<rgEg9J;0Qn!tMd4Fk6h!1=nVe0S7<r%VtEcF2~KMgb~$xJk_4V9>#JCV6?@%;m4I$j5V-ISB5m!4gw5g>|5M9ahzZbxHubKTi>xBX<)tWl+dC2P6!5+V?(fUhA#gfyYB6ygYGJZ8c4kLwlChX%lMp@eq@>J(hrtsM(EhIWjbg_ZJYmRgA59<$IYjN5tVTw^Q{rFJxxPZy}*$FFDmT_tr%$FRk~;tVw0k(fhL7s1IhTI6`FH^=@&4TZ5&N40JS3=H=hwk0c9LE9Av7-mDA`Z7-{QQCsJU%iXn`&wX+*q_(O8KAtgH)((E5cZ8W%3h*E1vJ)tPn-0cnu`G%a1`5@LO_t0hoAp~ShF3dnC;^Fm!q{Jp!r3m)awqjsb<!BCDRCTy=qF}2UTyIpX^K4<1oFOin4YIFqn+;G=6<wpjYyHN*CZ^CErH8@`N;;)^W6m7&sW&{s>$jLiV9~A-gQ8uP#;*4d!^>MK6ywXnJ-`zE=rtoPhx%OmE)o=g<5?fK1kHYx$?#cA3*QdV()5OX$?yeh=^?gIOE>ANDTu{5T}}6<v>S{>ae^^4FnZkm-j-DbdQEVRqNld`r<j;W0*r>k@{hiAGr&^c2zr1^<xP~fpjHgc@b)sYd=_E$dURWe6gz-9qzPd5w3;yLo~i3I?F4X0XMH0PG$WS()^nR?ISn$yNLzQFQ_SsE49#tovaGzRkZx|pOy)L>{<JRT1tD$M^?aK-?rrQ@i>%@yc2N*^F?xhu$siLfvL51YfV0Prw?2@St8{l<r5&*wk*jT#GjeT1fMJWY?)YBCmS(Y78#%b>g&zBkC7{Z~<-uki)y$_nYN6eN)}crChU8To5RzBvVpf_dMAev59oTP@lNd&0nv)w|mCH?=V{(EyX1)283K`MBI|T<O-t(!SVGNVg>tH;@0%5Gu#mMd6?2J(yJYr5CCJ<|8Gb^O;r%@ns-kcV>gpl)gen>8-HqFST(#MLY!CEm!Zj-jpAh^gA)M@uDvwgMPM4qYx&dZ`zApaJvU5&^i?l+&3umBZ#T&k;Ydl*YSV@w|1mfk02-nJEkp!6z@_e-wEhp6=fWLfC?0Zh&e1{Exij;od{mZs(!9eBDzx2O#tDa23~?ONUj_8$o_D*oiOYJQO9ZB4=e@HW?Og2MN66C+ZQo7CUe(=xZEnOKu5XY38&6lBm-lbE7HM$wMKGkD56t~;f2qRLKmA<f}xbaCv5EFZspLsH@tq@0ZyC})-0W!rLLy;Gs<6OB?_9Ds|#FQ?(#w~!c7goCb8RP&(~0E<dI$(eZ!cRNipo^h|z2*&deTZm_s%KeIZ)+O&%K-u{oQ3)fW$*rEZsN;ws#80tbd`hK6Wt?olKn|HaQJg_cWzL;QQ_4e(l`?JB^tAAp$tPAp^l?zjFg7U{j%ZnwaZGG=E;2-MzD66S78|1k&cK)Pp@-&{7F2kIa!52&kYs#$beW+H(^i%=gm5DU2v;eWhp+o)H#)5oD~noWx^A8f+UPr@_@9B0WWV^73X4dA$jcn!meXWrIK#%Q#TuN47{ghmZl$onT2*0dgwiN%w0=@p!4VL3lwHt!?$@0XJ46OV$vJ>sa^s|Q`S{~D4eK=Q4l%}VleS&32)mr%lNP;Znb9a@N(=n}Bjvc9SHmI6w3&(rG3{~lDG`l`qKmBKnWHP80^^$YoKY~MhuA_yt90Q$$&qSZv&?pVuH?iFTqIODj#fd?`Z@Zz`IImw2&0z>VD#1?7@4%XK_eywMy6$t4>_+I)hz3cD!C{_c0$gq>GC?7Q8W&HTzpEpLxWHhWM1>BmkC8Ocd9>)7^1jK<NbsrD~gROib3Ll;%HJ`eG{0GW9l-?grM8MS4$=XXN#S+J%3&PNq8udgpjmm?(xZ|G&O;2V3h2TV43GCW>n@mq`c@mCm*Ahm1J4yvJ%QBba>k$PnEl8D&DnI1=Y)rNFLv+RqSk8nozQCO667akw`K^(m4Vomd=edYXuD2#?7_t<0{5FS*0tT^!I@5qa2?`8rke6+O!aNYqhi>6g<+-{pM2|R#52?+1s{z5cBEu&>XkIsV{*F0<jST5D#fvLck!_h8>Ke>3O<KuY4RWYoISRtov=Jgw7)9T+>Y7o;ou_XWvlY5!XX^h%t0kx@bbD<df<XlaIYmO!XD4g)TY540)kr#z(JN$91Q~4pnX>29X)N7%fmcS4ezywL8QZyNw8MRbVB@dN{*FYEdaVx(qS38KU6v0oPGgIuzN|n@<TMs@K@;1LjaBpC+C`424&RaU*0Swh+XyQMoMoSt%H+jnHM8&=AUWuNb2@jGh*}EylMNl~Jt~{3fRa7Zr5ME9tDE`E(j8*zjSm&4x5$4P5q1D$*RE=ql=^r}!u(!zoO){O32hqHE6I<mym)#|G;FzWFpw_?A9ZSEy#O1-^=*1>U8L=jOthr3OBhQy=)0t)ydlhm?Vz5WDAQgzA3XDRmRobewnd`idO*m@vL<rH!L`sJ-2=yNVOaAvfvpwuy_aUe`_WgKkQ&lmFy^rw6#L_1J*war<?rTzr5kIo^<)X72LoPNC6bmjh4I+B~jet6*ohSIK3eT;F|=W&Q30l8IoQ_-EODaNK-K7*QF=TNCEkC!a>3f_~d?kTe)KF&PC$zpp>IZ$2qS6~>YsbTD%Lv$rtj04d>K*7mK65jp<#d$o#OhTKY4RCYE!mRr)JI*!kQ&g==jtADEj%DQXIYEV|OQ=lx2`O98&BC8j2$PN`bnik3s8M~~IxBVan($yU|pHd@HB`55SzdekBXQrKW8_U5k?qbET?o&Hv)wAPW)Z0C5)Vcvu>@thFtFB2*@bj{3*gu1du)%J=Mjfh*mUEuR$+`T;=7-{r%0RI+w9I>Ga=0+OQ?l7t9U8dIs};|I>`l7hA~eB?+U666b!;Zo$GH~a{Bvwq&&|}*Dlv+Sj+;-Zz7E5f2*Bu!(_jquVGQ?RT((dK7pxAWw*!o6hOs45SJtw%LwC664qWp&q@jr)`d;EuxhQL7n#L-na2uH!;WU=*dV~+Lg$SpeG~~X4j^3+^8olckHHJ7fX|cAgjrvWds56W2cHa4P`#11o-@ZJm4y7D2&DvT{1!2k3Q>PDDR`EPka+A81(N!DoLeIaHzcP*OvB9={*;rLgGu)hW9*b19-h4`ni*|lx+hV7HX*(SPCZ>J34%2A0VqkF_>HQSOB)tM;;!puHbIsP|fOQr3nl=@U2*%^)Q^J@KjFAPv80}b4s7O%kb!~vLimd`8b5|<DFb2ICWWrDkGI3<vrdp2xR<&)3Wi+9Fe5>jpUCf_V;bbR%>@Jc3Dnpj6n<Jon>c!4L`hpau3cZODRcMudMyFP36xRoUtQ!V^oSO!Ks9iOrEQjjWf<v>q-+aogn1o<Vm5FhHF@It-!x#&_Ieyw~#n`NF($%D_3CZh$Z0cuaP0l?>7=vGq9kjV*{@4ixJm$2$=m8XkESsF6PbijCfMOcgAoeJxokANc;~~A@iyo6|RkN(OswJ9<(@+=1pJSpRot6V62!fH=%r-TjN<uK^P?L@391k&ejvG0%@hccB1C%l%!d@Q{naA{GIY4V#>dEL0o)Mw5Hf#k@?kNp{GpoyXIw~e_@r={z1dSM&*G76ji@dXX!i=jYjPUS$Vpx_dySU9KW4Jv#G$_`KPYEI@_SkG&LF$ohE7=Le(#L9)X^~b8K`c^Qq#ovNilQ+1ZZe;sS{9k>=?U^tNJ}bFa&3XW#_1iPx0_Fis7Ht<cC(?)r+!w;LdhGI&9yf%+FW~+-ff1>pI;)Se7DKk@?AOLe$1eJ{@rTf^hIPL_!_NWgeZ7hbCj*$;gJY~l8^Pt&imfWsU3Azj?fg>*&8v?S-Hiv_jT53dFCc-4R1M!GAe4B4_FRGw@rHz5yJb;r&Lt9>D^G8O>fwrb_W>K)?lE*c!)8KY3xqGFnX)Q=w*O05Kk~Vzv|Qr?QFYe2xB;J7=)@mA-hVMVI*NfFmfNsj4)QQRbb?8c?{jvMjvz-eHdW$J-1ysk;q?EOTXTBM&u$Y^91Y3&B%c=&y{x&?hm<%(f*K|G%W0>_o8}A@45GseyE<8BkHKpA&Q~v07@v}@#n3cM^xqsPLLgSc6zU@B!dHe<N#w8Lj~NWtANMbJ22W2M#mG1VQEHt{%wQIXubH9AfhTyNjT=HHJ|zsM7#Au8inb#6$58@m8OLib!>v_3@L@)8P<;37Db!7Q4#zE(w1DsS>z1GoVE2Q3WU+k9FygA<e1!%@Jch@RScW)Zc<)_rlKbwPKM;8B!d%-DJ_(dq-*kGWK#E=PYEL`?L>|shb*T6#;H2f!+3}-)W#}}tC)uGm{cB3>a2}EJH8cDVQkwgEeuWliM0_GcVgfGv@uM?5d{giZ}(NSv5J$Tjc$98^b5}zql)6bKKxeCNj53HcUw&ct711=86vI&*mV9upol6v*%J9#E6b^$;oKGvC<$XD#y+x2X%*<O(y(5&(hXWkQ{JCidI)7nOk1}B&yu&BPYGjyFw#8lQ%-%HVcgo7m1oIDjA0CWZ`S?1-`AIa3}anL*T9(iLFX;fw5yG=)HYHYz{7mI`IIoC@=e-j`3D#&j7!rh5-i&DM<do?WV;x8-k{>3!$@I(k?)vS??|J|YC|0hwQ>Jmt)I^Njj$k5;g5M}4|4?79B0_g?;+dij4H9dPrVfja+$kS7N}aiv}(D>)YkWl7D!2CR;np&MZgKdbietGs+kM|(ecbjP)@^Gl5^Waw9u#gHxRF4_ykg==^N%1h@|?-x~;nbaSmR^D-}8K`bBsEx#N4aMl6?%l(LZvQRJZ%J&6#I`SVCKkjaj0z6A@rQ=%gq+lYaUJ*4+1Ny^6=*=tBjB;P|xlRjqiX;;hDyX8IbDGS`95%+lG^ACM?NoD=4d+1YE0%aeUy?~oFmB^3uz6B$<uSz4VsfSpgrn-&H*)23xLJ}3=Fyttb+!)f<UTjcy?W#er?7H7}N+T->IuCX;l~ZqL=)%t1qUdHAX<a(#sx+;1vr-RW=*0Bz9wKARBedKmV*2IVgmklBd`b{40x`#7W(F~lgc-#6hEtizQaoi9lj13>boB%xUeq8~{1EVnPj`aYvW#chXUDBp1{C{%YqY)r$0mn~Y5UJ?2Of#FgIM!CUAvtETe}VJC`G+SY@x!YF)LMUtzjMO#68x*jrh8bBlL^5I$bY5rNV+5kmCr(eah+7)9kl>_qL2lB8S<Cfy2B?SC3ty=O;3%Ykd*#jjr=c4EvU5sH64fQ-TO8Ku*e1wF$&>YG)AZBnY;;S247@jr6`I8jUY)lf~Y3n%ZB`!kEq8vXIy1Dgxu${pM4`7!i!t*f|2qrz2w_Z|g#8nyKjGA;!9Bw@y1+I8Sy#%}&_)K09IWM~}&z!s@Zhv=yKL-z>p3S}~|R?;0DOE%x3&bPaSu)gYUF)U2rGG|UKZ3q+LJgR3|yiW>K~(EFuYsa$<Zdcs3`65>=!a9sly?xk$&K~0BR=QB^cCzuQ>2w4kgn)0bfnwqwK-K=TqA;y|o*ORPT?JSe%Wj)j5Sl=T1vR+*0Dy?H>_;3QQ(e{s6leIcVXA0f)UxPS?Re)!Bsj&Nv=$6Bwb)@VLsgwhdZsaCLbR#$EVp&Dmv#pFFWKqXpnLpNHbk=n(?#d?bt`X0psrLQi2P&wFAE*KmhcIa;aU6}TamRyK(D19`sNfgI%`y-_C)x;m7Im|hZsg!{wj;d9<FoDia}*2Vo3GK5PT&3$9*Hbk-&NA#dE@2&8r307TVIyZG;<d#nz>6C8)seZLc+*0^~efM(59bZ8QXsG88s7a?uusin;dB@@fB(`F65F78xC6WT;kiM{xdXF-B+4Ly=8SHt$Ie-{2Z$39?hz*;)gb`C$aO&=Oi&8{K~E$$Q;{>t%6^9z+MbMZ_7Rv{OauE4Zn#@Er0Oqbt7$;f$@l$z&)HxO~nr}_3-h#b&Cy*0r*KF-^Rmvtr+WHm4*d=QD>;kGU|O}oER!|db(z){0s==!x<`73=u<5jPtZs;a3M3hK-&wXc!)1jA7X6Deq3nx}Yv3&7$51hS3H3u16BPz;4dY7^s5tHA-|t{XdUH7EW;0XOeO92OwK!B}HNFX~huPDqZoUxnzQ>oyj<D(V9!MqQ)q0k%fFttdq{GsHoZ_kwLMC#w9Wvd1y)V@pkrBJl6=SbRelfaC&xDgEPx|I$+Pi4jB5va{?O-`X<=~hS-nm&IlZ#`i_h9lHeRH8k&)ltxEv5``pB+-RCM@5#)P{Nkxug&j6fZ@)L4C+64<*H#$Oa<pH|pGt8`%t$L5d7ZiB%UxlYM#P77Y1HRjq5xfAQ6@xJLDqXCX@itk_hh|w@h0O=f@KEFNwgU!-L8j%{@_@(ZxcHPh3W_|@o0%;n?n{<h47WWp<Q2-vRtzw_N*6^sFIKJ6Wm)K$3cdIgR3c!{sqGvgY;$9`&XSSMov+cJP4XYmNH!ehKXY32P^dxGC#!&&)HNTDgiQslxJ|}-4eCRTp^jyEsNK4U9VvO;k&+!8skktjV;9pR>gBd38x!=Y;2I5IW^ZY0ev}mbL*WHQp=2XZ&z@;FiUYjcLRmbi*@_9moA#2!eR%U{sZ|wxpHFsSug|&cK+Tl{h9w1%SZTxFTiDTtz4J9%|7}i)Ep1lNk=TbK4a!F;)=E*2bd(e)q|1nVPl4AVeHBBbcj>D9kUt?c!6E4*Vn=?Y33jG7tIU|TH0>z@<9z*A4aX3&dWT>o<S*pM0txC!R0gT0wxR8GR$AiCtisI-H!<3*&`R%DAz~eY!j+_MB%#k$qw5L`V|~fQCT)d*m4v1dC3tbDzBK(Nrwp<J!a0!V8RtM@VD>H#)v}SZ!&Tv}82H{px~xe^l&C(2#-5Wcr`j?WeYxZCg0YoQR~I(OBJVe!62^!gv|XAvDG+%xw#A!mmq{JmH5kT2Oc2IUSlMW?n%HUW3Y&<Grmf5)6|JMt_GIGs?cT*Y^a=y6`Hbyv8{f+#u}%nUU)Q9LSch@OrNX67oV8+5ce+X!TQl4p8pSL#y~;`*TJ2QUFWE3VGOx&j5Z-S-WeWo+L6Ky8HbuqdG>uyt%hH;SN*}`5if0&a(ghEHP940~#*k&Udo8t7C~P_4Sc#==0Brb`Z_w?xof0|JJtQ%vI3t&{wJ;;+x7w}OkUPW}Ils45-}RZY&jcdmBvq=TD{_+R!oLQ(9I}ViJT^=B%oahuimuV})f}>wGUoPcdT2R<QG1bf^+W0b&ZhzZ&ME0evW6C@He#?q^(tNDF{N8wbEhJdy85AHT^*j`N`{eEe2KOViWvFO<K|Q9sUR3V&0X{PbY3fFb`ogJ%6}81uK8UWmVCp)p0y>%S!PIyiV8vUP-4oy&dMrrY1dYV<UyEw+<ZzzK_Q7sdN94)lv6h%O4~dNe`6p4-%X4N_-@j8UoqJ`b$d;g^&Ljrw#^>K40^k(8Y^aMpWm2(Vjef25=Mt$EM<=wMk~QV7*ikHj4-C2W^sTqEwdHg_m@E!vxaoq$f+`%v@pgR)_~eU*BTEVWsciU2_32nxftT?8uMuiGjy^ucpD7eAtngjCS9#+<VZn#mdP^LJ<D*c!B7iba3YJRHEvrQOt4@n<8kvTVMJx0oY&4oU)w2w(H8H9pIllo*1#%V8PXg(M-|2xW3Pc6Cl~`R)b_lWsCakWc1q|_sV4*jDzhQjI73$mNL^1x(n5C?Lk%R}ck$gfo+9d!nk=(j_K`DpGeXB^$+~a*S-^l`+;2W5jHrqend8J%QclyjOq{p&ps4gHv|^xvSLvdAh)s&B2JUNyZ!ngHZ3B#D+0bglC`q?G0ra38H=hwk0aY9}9AqlSmDA`Z7-?&1Z&~s>7_VXoBW)e;h8F&ioNh?T4u+Kc<PQyQAL4EtxBea@bmnf$&;v#{btDNP8Iub$l8JbD2_f0mBF66hu|cvC1G_3m%h;l-!xFq|R>NRbtM_bil$;_idJqa{PSbgdimK=uZQ`$3;I#j0(?j6}HJ#GaF{h6C)El1R^;^&ac@SDLDB8)+g=rRD2rO(_#h3eC(;0^7N4FViIrrCYe~VrLIG**POVE56XL~zqX+8}=OVc(Z4co$6v4vWicE0EZjK=9|YDC)##zGV)L|Y;+za$ij3_U2gMp0SAHd~JRODXO>6l^q1mK9-UjLN6=y`TrXl;K3_9BReTByX=I%O?_MuZOzqhftS8nh<7BtBEAn7TRmvH6a|*S>H<p&6MT8_1wu>PJ_%a*3B1AF}+tYG`&^Ivhu7#cT8`ZOm7%{YkmJfTNb)mB#Z6sSu3vMA$Cy^c5x&^s2_hxCm}!oRm_N=f0y>`{fgUkcd(NYyAe4u@r(9>HQx)3DZM?`6PHzB(WQsB4G1Xtlw5q?v`|jRO$%iAwclM$=Z54}91xOM>0-8;DMZznmJhMdRO<eq7R#LJ@Nx-Qn{ZNu31_|elnNQqfIKxMI^C(CVGNVg>tH;@0%5Gu#fUD~i7kwxlOf%l(iEy$4j|fV71A(7&YROBmk@H^&acYl)TSA^G?CAC$Q@#g+$O!h>SU|fsMGOW_O5l<naESAz;*eBvc<Nws{wh${pM2=9H5GiOB7}p^JyFtNw=l?Ntw@W#ULoXN>}TpePx-2TWo#soeG*@MineCkgN79Zr6lGi<X3gTeLz@5bF!0KyZb(H$D_#R0xVf^Xp_e9m)1+n}at&p$EE&5edmn>hJ4mP6CW(Vspe9yDoN<f{a_>_eW#U^3jc66L{A;t~;f2qAE{xA<f}x%%2j^*x8NY8LrA`#Xvc$)GphZO;U&FO&a!ur+H0yyVp&r;hBENB3u(bU!$ldhxXrxvUXWi{K@&%Jhr=?rWxJ1*KY))8^?8E9lBL2iv{9^6H$e?ZWL(nl`!I)+;ZPEw-KyC)Lr|<r&Li?)XC-yWR%Gh#TmrPjz^k`9%8JbX{)oRh5t;IIT(ZJ0}tkPY+p9XwqX#CW>c$yfsmGhYqa6(hQBNFLpd%iDECNCf#b|ylHujie}*!ATN&06z>OFHT&28VIW21Dv>vT2a@s6{iGFmXQT)%qX0l&=N_|C)LgZzRcgtxqGn`?g<zfxaLyY09Qn%9AV6CdJfo-p^(fUbW1&2dD&joa)`*o+p4lxN)at>gZqehysi`(?9)37_l7`si{_QxXZa<)%eC`;m##yC@Y>jxMq2kE>@TUySUSXaOdh#ohe648kGy{P$nbf>_$ragfajOZb@5YZ}Kcu#VqTGuR-t^<~wn1Q;43MJAi23o&UA2**8#spz(o@3inYY>b~S`ncUlL8~tvipaeSB+|x^+uIklp$3qtJzjud)yWX3>um~E<Pn!qCqGMGOzj6%Y-7CJEfpT3{l*r@qRu+uJYky6zhCei{fZfU40+`8{eqUG7)t9_bM@KnL|4Mwaz^>wRD7_v}W%9$)_~6hiqt+?2us5=PG7Y^f{!wXh|m@qn4FqneVa^$~JU(=^{@I+1j~VECphm>vw9*1;&Q7|Kf=Fp-3`9(m4VYnE5o#NYXZxuB9JWF;>bdT`8r%2V@7MPa_9y_7ZJYh)Y0i97G;XZXGwD(y)R;kI3H8-D8+fr&s5=RZ+dN@wOEM3=ir3eJADftE^KPM8m#^k;dUzs?3;QWLWpxP6?ew&}AnyXaD)snHf5}Eo$=`bcYy2XU83I5W1wTs@Y_r*T7Vt#9HW*BTT3lTSFSq)7Ej_DX~M98;N0F_$7sU7O0&o48XeD9b$}~-xp0U;u+S%nL+8YhD{k_YAvJS!2;Lqb8{%NsW+bzL{zV_6#N{?<kQ46h@p`0Fz%3S#1?`WHolidqbucDK^v~ivYti1?By^jT~7&oXhH7M<spessTKStrvw)jbS%4>=cSxZ;|3c(?6s+pMy!F$e#u;#;}cy)y|fu$kF%EvTeAG;cfX=*{sy{V9V+$MU>(3ZpQZ`x(!lBpOD(p>S2484yL9mmP5Y*4h_&U|+hbd26uMFmX`6?7okg(j*PT*NQGLgGus<pXLG=Z%#V&31&O;sUhTT=1P&&Cuhi6V)c=fuTitjmkNP?YwDF-}aAT4%++Le#%PPst=Rdu|nbHhD$Im|aQW0!+a()vHHVyj?hw+G5)(Oh4mkY)W6g`820-0Z|Z%Myj-=2OCmDmvbpFvmgpGy)a$+oFV|!MKUZC@}hckHUTPNhzu@?wsB?7`gu0TNukqaNOkD?Wut^o#T78zK@uLTw8yY@X!}!k4ic|2R*YV^sfG`1}N*!E~`OV#ZG~;knJy<(21;G(<%Hi9h5X}rXezRxtYedDhWC5j+;-Zk*KN@nC8~yd^+>-q}#j>hH)1whIJq3F{}2SS}apKKST>&O^U{=NlIzAYmyZF)a;u!fnWpdyqm94CojF{P#D9-#`IV&S&vFWvGlgg3vH4O<Mv{+uS_&>D_AR@1KFE&L0D*l6Sd<fxPGsc3H70_ML2)fc12joZDD~>Ty)%gO7(RZ#zX){XPgFOxDR8v2jjB0GPq!M7@ZwpOf!sz7F#o@Ba<~KE<)FQ2D7NM;h|3_9+i%=My6@rGM`2=Gs0;s;q?d~Vha&YJGIDt10B6r6*YR>D{2gJYS?0J?OR)S4^TUg@71sr?<oa}k|VCizJhsFD9ZjzW@Rm>f}mvSxz-0Ls~9XWzDnK7=Bhn-p??L-Uzx^6*<jngI;^SGt`H|@k*3z0Pib(`wy<nk>=Z9;r{fZ8+K28i*H$YA2Dg#k&tOc_3qU3g1t2q*Z%vL@S7C444P*j>@woYvFeU_JWC1WnJJu5_5)^w~8(^$rtH8*-*a=@gzo6HGOc-iGIrSLpc(oh>jB48*3qzjcTQ!^N*toI)IRB^5*FAJEBd82nvTlxm@~Ia)Bk2oNm{9a4MuehO`WanYrDt5946=5Z403Lo45D^Tk+N5+TVf3D>VESn*J~1jv8ADIPaVxL#=?P)pE+AGwyT?THA0)iO0Ng9si&1N(M)~m!7nEe`Wg{jzIfbxN*F;^$g;6<YV_Go0gP!}q1eNicA{;pjfeDpV|vcL)pEklZ`i<?rhFvkX!7S6FG#0l2MK~;%s-p6QROoEgka3UCL7H?9%AesHx6gxS1?wdC}m!Rygn~75AexywAQo?mC<6w^mNwltpMshw$Mt?idjxa#pErbaay6E5d-_$NbjeScNSEbaY2QtYoS^cS(Zz_xJ@W?)(YtH_lr*nBB=J*Y+FI{k!>s43B=OVYLwBDRt!NbQd%S*=530i5dCg4m!Mh}nd^xQ@=-`j#!>QZg}!ofc7Wb)J|&_aA)46DhBlx2SuG25Z&bG4-o$9@?M-^OF*bjGiIn=?CTr_=<$(K{gYx;OG_*4R=`<qv8inE~g|ew+`!$&d7kvUK`dFXryg9y{+EHiah)r>!y%7VQm0MtYUuT_`X>PLC@RoxpqoO+7avbuq(4Kg1p}pOFN=1cR;ti$Q^ajJ@b0=>!g+zB?Jj58rG`1&T7`@eD^gO^Ah$k4GU$yPUcFx@qQq=wCQ^JVqJ|VkGnPDVhLNIb4$&4^ou~lH?ZO07V)kYt57=0LE^gZicIg!X;R7}6#c1GkPs`G?x@7shNsPkMI3E`TNn;5Mbxk<yqj(RVur}UnBPw9vHc{!q%zz27cMDC`aQNZKRTR)Ge&J&y<JL>H8U|H!02V%(q#wvyixJy?7kGFSVv?Gj;CltfVj8aF_4YJAW#is-jRe4InF-NVD$d4e}tr^lN9I&kzIK!(nEv%?x6I5qNG4#%`w#>FD+SF0@;75?Q=Gwdgwa!q?S$lt?Kp5T3K3Psj_Q@RyuXN*G#jqRiCgoLVDthwaWJo@o?ccIC7*kp)V`p?7A(Oh_d`cKmX(w_7Ib=BnFizE(9>zm#p*B`&T*Wke$E5OTQfF=Srd<Y%ISf-<I~N$5`V(s-D(=KW@HN92CKic;gxmK4E81AaNzq2PJxTh7XN*xrvCcs^^_*mr(tNkoWUwlB!HtJm;(YNb;Y5|4Y>E7=mF3jWaBk}fl!UPnV;@<iv<h@s=~%B?>9ks%7n(`{Su2-oW!e%Bc%r=Bd`d(EM3m;)pK|KsjOf<VtUOURVvK0m`?T(t0DNr&z=+oJN&{o+2cWm0)2>#`61N@CfUNm;^C@9Om7J9E0*XmtT(ee@V9{Pe8nFf=+r=>S1{DV#MiT}Y`HrRaPGJ@0_Pd-wZQQ?CBMY%v?dvaYqtSmPte~>v47>SlWILT<CDu2vw_-u=bC=2jR;!m;Z96Aj{&(#<YLS%GvWO~Fv29ycWsDy|-ETglZYG03bUYIkl+!Sl=iIhKE%fRC4aBP$K8I9k`i6N0BB?&plJp?Xfvk9?Iu~45)<Kca@x9u7fo=Xx51kH5gn-PSN1B05cI5mmSlFF79U0k142<j{y+2k`KF-KqLsFs{^pPfg<kUu@cpZCccl<B{72KjR`sAM`N&Bz<q0~$XRDc|jbF-on`H}UvK;-t_YJ?T_5DQdPw=q7ug`!GGqT(CcmA`#-E6I(~ZSBzp#n_Hp1QuiW+fHe21)=D{E~j$p?TljBsa+H;4I}eQhhmkcm6i&rPE$DbuOT91_#?F3K4SXK-3A4;_2N^4Xc35sW^S!=Dq#jOzTs3xwG^LO#iaPmDqTH{h!-`U72k~n^mzJoC$yE){IY7qYogp3J&JO{H5wI#louM86`h11OuU{P#GU5>-0c*Y;BB@?Df=~I3zavG$*N+44ePiM6{ET@xC7X^u<9IHZG^gBd`jg7RU*d`j0Ba_sqMM2Gi@!eaC1;A27dG^T|I{h-!Z^`u)o`zd2fK7Ut-vC9VV!x_2yH82ns_^%0jjY#Byq95bLxEw!l|0w7`w@z9t%tUTu?w-i?~tfzZNeoL$!UI%CbSiEF?4lrTmFqcwJphVto1aLC)Lk(zuex_F4OF50aZj~0%UT~KotuHp9$M)rPmncQV0r0`U<HF)@{39iv7D{??3PEuqk4{Z<a#Y?D0WV4T&6}6m(8R2c+h%%jU6-Px;<NjKDzk(~3+E2+^h600xI29ONkY%@}bW<kK-iFie8zzIwL^&>-nVj;eN1B?pz2B^9>LJFOT9=uun(-`?=yjqr6pDC?Z3}VQDsO2GF2VN{aE-RDB)Du`!nSlE#J`4~S{2|KUMggOBf8~qXf7$MMk*@-WF)zX5hKY>y4Y=z?U}ZbFN?ZyK4;A^Uh8sFv)l;Ys&KT3Rc62VfeNbP2Z}?)4fE>lFpfsnxZ@oxX!uoeRPYPqA{vOF6K%{ri$q&B<3`FZXWK_lg<+&s(nt)UoUhR`P#4<;w(gNIqs?DsDtKO<8EJ-@wl*%Ksp&3O)O43FR@b`Pw1i=%)I%#YVVjm)-8x!mk9eW?i_fU0Xz5opvlHb=W9_d{sc|8rWZ0k3is#zjF7=<Grt0ETS9P($Z=__;;G3UAH{GKd*;V||3il*--W{DJ283VP69idjTd`H}E05fZfoWG;o*{m|x2zNQTK?R_ntQE97<<WwQ?IG;Az~jsez$HVf-wL-DZJfyl&=+I^{djbfG_HVmZdJ&D`1=mEpw{6Cbawv2;;*EEmaH=qfd<UY*(RX2N;Hp%`#{h9%77P*x4-a&eXb~E;cP?xemcFdO#ofXV`0su2BZ}x)<!y<M=>m;lNjYKN$yw0JLSaQWTn>Rt%x7(iN|oOD3r5nT)tT@4MlgW=)N;{c`)Iu}(U#rlQi1L<W@~8u!m^WTPeL$7|kO@mw&d(t*<g!Rgsq4bChxowfBG?0}&!yeO~-q0J?e3t;B`xbBR=5i0h$IPVwE!J?rVIoUc1U`NnRj5>m@(iOSBr<hdaDE5J>-zkJU<b1SC+O#$)HDI$ly5%#>Y?Sm+bV02r|4n#GLi|pmJD|JmC&8-|S}_P>uhPY08gG-;TxgaV>CknChjNd%9WFQu;%;Fr@}kGhr&Li;@QL2c+#+$`&(wmrt)L;VFjTf;0OD1;DA;+iYQ-+gTo(v=@hPah!JbpwQ9{_{#&%r|Ba1uVqIKDdaaPjyA2!Dyj|3W&d=iV9v^5`&<W2>vxJ}V|4d_FRfsSR+sNL3wy(oFzixLlBR9qNsv5RRD^K!clEC%RM!8IDZWJ&`H2N$#1^AC-A36z9V{<BRlplLUX1HRiTT0GX-iV5PI_JYHGeDjB?))Y$XfKs~kIUXK(ymGj(<O}My`bB?hZ)}6!`5LW3H=936`>%=pL!k!MqtbRf<u~~-#R=*%5Z^QAbx>c$5b9mJDnF2@0dbb~0kI=LvI;v>8&_sfLmVngBc#W7YB+`v#pgu9o8+u{tVsrCq#P^8d7NcCot2h&v#W3q!%d9#FtpP9MTl63q#%;9qmu2bg3)zEdSiLX#3pHlft9qZZNZX5<)!I2Ic1C$5YB--&o~DP0}pt4vzCp@9WEJf#lZO<(q%<L>f8c;7Y#{U)R1#-8Oy)iaeu)mOZuHg5oz*%^C=OH=$YH4c?Cl;G-D6F*_NQxfnS3WJ;VeN4Ta5(7VC|j*2GXm9ph{dDD%i+>nH~}s}fW1ZP5aGbj@e%jkASJ<B?b=gtf1`R!6MEI1f|dQm4~eF(^P?rHgeN?hgH8R>!Dnuhe1JPJ8{5P_v!q&z6A@*!#_=>~R2PDUxgfr&zh1rg7_IS$wnc@k2yg@r>wAx*+qnB3|oruw_eg3!|L^WXk~uQ0&?cK?cRB{jyU6hq{X-#uR7Zat;?};QUt3^%`)87z5|`mg~EYRFYL+P9;@Flq+zODz#gKTn^*IYK)tm^acGHMRbjp#fHM+7_1=|oc*f|r|Afz{v*2+;PnXSQvnF)M0Mk7Lkn3OF<8iYl`fK-($21VRuKwaRk3V69p2`0g2HLZjoO-6gWTzH^C=Zo5R9JYw)(QlZCXucb{=WW?0*xZw)$NfmXyW9p0%aP!a285-rx$y@=$rozOEhK#_5cA-70gC&Kx(N5>ZfIqLMUBZ#m`Ejfm1Vu)^OM$l-SrBM!ftG~QQC_D)@alV#HR%xz<54`YseJS`NH+qcwM-vkC2kDE^kqeC!S3A19BQwL#8eQYzrn0lth0migUTzKDs24OrFNL*n|(GKYf$GUm~R216S2ffrBx1AC?R4pp~-m`1Wrzy<P$xZ@pFm#8QAat8_weFE41?{aS%cOg&;aI67Tj+ulSv<J0b8T-RgBP0P=2OCmib0;-%oVns0vK)aZuljp6=Myo(v>0IS7~4vW4}y{6UPb00OM9m9d#xu=^eM75;|1%3BiDRZFcZ+hOTg!x}L+Nh3+bb8c4kF^Sf_6bKa<>A6drQBMVD3BZ_R69R9XnE00ze9XFp6QB>QB%yHs6DW_@NP0m~QP*j>2S}{<@t8~#^#3n^m8BMaiGL{8!1B_*N(JI~OhE@o=#{K3q!YH7i!-j)QCAxAN-2@|T9q%pYUI*h<3}K|LA>Po!HImb9YR-ZN82#iL4el=Do}53TfYjVwnfib+PaR1@NXF#CjASAno=QlzHH)!(pKOq9#K6SL(NVUj@UR50TG+t7RjrP+#ZjbfoW`BGu_a86sJ@D>(WV1!tVA7LhPEF1axI{|Q@TFpEHa;Z!!x{oi(epTLMsLZJlUBw&7w_#vy)^MUmjb38muMy(Sx>C1m)qR-O3QHf^|IWOPHWZvcj)2D{MXuKw;B1S`9nMTCs%+n|31V1)|338f(;bV=P2*LbPQC!=o+juIi8?LKIx1G<;g2Y*bv~B~7J?{h?r^v9qibGwD=5t<MNO*rhlpN|#Y9hSqs|J6S%QFnc`!=GvWlLzqLF5N1!S;ib^hs8(8x3E_~=`ivrI`Yivg=cdnc8f1pCuGDaf6~2n06|Pd2m6H{^V}(aID>002)<oQ|OSYTOw%FdDwGJyDViyHr7f1et`tg?}6B7Sl#f*smcWJ*CK=GjN4t6wRHzG#{lf=h9*+fao@x6*MrFi>Sa@g@uBtapF%bU(T1e#CB6%J(gjpALU=Z54}91xOM>0%0-DMZznW-Hhm(;T*r#xy5AD=G|XD}FJdoz}<Ar&P#@M(L?ptKOaZ8OAU<y$;4hED**jT@3Sbo$bOX4)ydL4NV}{ga?qA^nD<(YT(UjkxK|UZ|Aq>a%$6zT$-3{JLC>AMsAbd-=VS<Z`3*ZEbEC7a-z&r)xdR)SvX$pmNLK^-hT5bsSr@{$0Z6gjQKQ<Ii=gO{-liZwqg*JUZtzO)xHwU!u7a5*sf*31T(5&d6!%@WpTSsH6rhbZqa%|lF#vjrH%O6Ka{G(qLNVD`z!{s3!#yskG6?<6BJURn;5Z`+@$`#nwsQ|W?@ZozUz0-W>d!X@`)9uf8X`H&pUE&*PT)~QN1U+kmhJLx;XYu(nhUU=%V4?jaCeFvr6r<^VuYI%)T_2_sGrb$Ae$a*>|tb4h1;q8YP4czD#Z1etjsssQ8ndnFoBg(=@{y_xg=sc;mQdtb?~oWwDvOP${aa$t?+LIgl^{T*gEe5v*-pY(zvD`^Bf!QB>5)_6%f{$rHsH#7e41nvNb~tfOfwxu=EyEQ?Z95bL(Z*1nES&Do2!VGu6rbbKaYuOq%jQB%(*n{MwM1@`<{2MWqPLKNamb&}!b(Se3Cr(22H5WtNX09>WKAV)1~=7MU3ofxGT&s0|MPG80U48$k<#i!I)L_S1b=6JW9CNsksHUcl!;5@__&MI{)eXX?ttgpe?USFg2lfFv)qD9EJ>rROsA|0aS9KbHQaSXeB{BfJKbsBbu7-P3d+pbxJUC#DNi)Bg5+zWm!DJ}N{jFh8vUZpMEE=FPj?Oeyrr$jU&Dln>haRx3hnpQs+P|}`23P$u0TZm|tF1#l>Qmt#2QP&qsu8!xnFqU_xRSdL#vOaD;C5#Ee*vzrDr`8}CnY1E8BPIn#rez}#Ij<U3DwF9wj8TSMg-Qs%2!fb9Hq!)QtT&&MR?#3B1=-hp>ScnF%$-_LBZe^U(s(~1$+~Ex3S%&FfN`{`uD%e=8t7Wr=pBS^|6V0%O*V*+mZ@xL<NUM#NGJ(GY0caTluv1D71_`z=^??g&sEH*>~lzY(Wy?pZq9{JmJhOnG8@t1sf#>u!n?=X5EVr^*Kbt{c-G~=MvmqS*+NW@9qMw7pv<We;LAM7eS0Wr8)4THkgFJLWtFb9(%(Tc^g8L&NLZU4MVlDntfSp=O;#YN>AZ}!pw?67P(j*}Pp60HxK&cUvO>2N0}v1C{Y@$5^Q#<H7)4`z4<n6Jvs8IBzsR)ix1AC?i=fMHXf6QcQ)g!A?6&UBYtS8H44oY}$wBCnw&Z4$xn2WPeHCk=OOEiYx@k?RL$6!Mb*ID*6>lVlc~zJcqFSJKuJ8rxYIle+c7ESny@+U74`_G@O9M?wVrr8_!D9xlt=K>i_nS`%A}ZNf3Vx1d@@e83#89|;7&l2aVhceG8$HaT?UjPD+SFZH@<&4~)2Y?y4P!e;Nw{~Qt-(c9Z3Vx{DZxcG9m{U!f+?rd@WF-;du@iK5o_SGU(%fB_(WGx&niDiDIiWEtK~nx0Tx~JH_!m<P_@Sf>j2har>5+%E<LQSP}*W^d=*1$yh|5v(X?-_hFIJB7<L2LRz`}h+Cxf-r!OBsmEEs9rJkY^kMm&jR1Shn7~i?l#sxi;;%?Yo#R(;pn{;^T#I;wi>#4ZdM?n(o<V!i=DT8hqC!vnz<GNGsTR?RkpZ`|VoL)X<BQth62qmot<SMobc6NKCT-MF?l?$Z>!1dToMDD~t%gTl0=2OCmYC7JUFvmgpGy)a$+ggRB!MKUZC@}hc>%x8WNhzu@?mX=`7`gu0TNrb&lyILb_0Ns~yW@Mc-j8_uHx(=WNGwrN$LFYLc7@*6zt(_dUH4@*EUVZlSQcvjWg|LeL#I}Cwo%6xOPUthP~RGfgPqvUf<WTC<K|PUBr59!rn!ANpUzA@={B>2Vcf-vW!*=5%&LW_=Ko6f2Su$rAVuTVET#0^HCYONZjMd+U0}!Pxp}@u8^m;CCGEfRW9h&>stLu?-!kvN$zk`n_}J{*77g4X){5sy_9k7h8k*ol?fS{SvIjd<-D?rfKgV`OWoTPkri22c<K|PUufs4V0x<52NDz$SK8)cWjLQbg;DXu=k|DMOjA@3kO(>OZaIF=_EqLIX&!8W5DLiy!+N0W0*2px?T;|hAW=1%THM}0-Lu?_!X(t`IZ=j?1s-i~EL(f)-Q_~h}TjHpjOn|Mke5VmFW0@_jbv~s0Uo1b?=Aueb$`RA7tmRY?lq@|p`v7GXgWbnhsax4xwGS`!rGxyHX>67aw&ml-nkwz&a}tX*wcdP6gNt^HWz%A(cxgKw_gB+Cc!ycIS}`!Vjr4vBW0GD0D!ST*y=Jc8njEjL!rpGxu0t>$H=h#5gkX#;0LC2P3xbhiuWJL0RcsX)nHTHi%jXyLVvzAeF(^B|v5;5m5x}Unj4?3eIlfi1sp50?<U*AEW8fa@9s{Th*%H)EHZ`Amu``gqAcdJmZ(_tWTBV=SxK-N5_30qX!*q~y({vEEla|UFs*ceen$`X0Q!e8q1Y@$oGmHlXWDjF3Jn8tEvlU~rx=B}4v?e64$FZq@=rlR?9AV53bUB33hOx#6*qUxm+nXLhQOL5faq9HhP63Q*+_%`nn05+otc{2Ces6kAs#!HL_GYz&GjSUEYAX(3K4FkfOArzS!ANXoqnb}8AsBP4$wu>zhZuXujojJz6^xZN3d3lVM(UH}pLvK+)+-NM>dMFgW+I->+PxJ(xyKg1=~*$$>8O~z1vE~p6EtFAUmNNDEb`703v*|Qg%KW}QVh#-Jy^%`6RicjdHcnu#1qtgY&NqX4asJf?1X1&a5c&xNh^kU7AY;#5c4)gQOJNdnIwp3k-476ARmRaq#~tuRcA6`8kN4?d`d(;LNu|PZEimGvxXLS->7WMy@}Db+?(`nlWhL{5-F9uP1aWO$^rKi2<7un-9KMMDuS<3Ud+S?u|Ab;80D$|q0J)!lz;M{&CV<5%c&i8R*o<gm)sjM&{?^~xc7C|X*p++dBp}7S(H&{oh7rJoNiN5GnZ{|H=j~x;iCD{r!;SxH$1+9^2T3CbO+Hxj1f&^(*j1++dGK1<0c#Y#1o9pFPiztwDiH8kk0NmpAtq?3<}w6$_yh36M~WZ_-2H$imd`8Z!2l&u44M2!|1~RqwneP%85k&q6+)<wlg9ZQS~QSPi{sIRDZ5ig>Wm$O^mjZ+@xV)QoR?|X?mA>r|E~HdO0F%7fv((ak{Z+6#Mw|7S$uF{sbq;jygNNVpd+lfpKzxv5KJr?$TB4<Lw<7WrWf3gko5l(VogXKwWfawW1N#eoF2!2eA3nk09EuMbaqbu&o$4!>cqctf*rXRA)#&^v<w0(zYntZZHq`jI@bO**K1zp_sEa1Vw=`x|x=;oQ|}VI}%>$(YuOakKRqnt0-0UJj97uHA=PpTi1qYN-Mo=nbi^}h^Rg7TUSK2pU4qpmgN*6I+bsFL=Ulridm&`72fb2>q?__ofXq}OG&X}61R#8qf~!l#Y82a7&rjM3=`8tLBj1zg%!oD;-o01+ny}_!b!%cq8R%SH71*s-n^|)gEg`H9t-ou`QlTeiON0M5cydt%c-By+?E+AsbeF?-m*$*74ops(O#9(X=c_7O~rte;&KU1Th}3hL2SMGl!yk1D9y7$<<!R+(XHcIdBSYO7}2n|aosNl_}V3a5sfsM+0+k6Z{eq1EvN0+@DO>y?dDU$h-x}1;{_Cx!nmKUBEh1)lr&-uMz)J#=?y9ldNCk|0Y<)KX}wdJM{WCELQos`?^TMgF~|87>#tk$=_6qUH6CZ!%?~Eq=?p8ezQw&23-X@3R2HyWz4mIk*G*fSl3FAswa?B9ooq*?XAr3S&1cljWDtmsXCi}g8pcAM+cvI+z7TK&@hXOIBvqQeVP1hqs?W64`$;>;vEr3FU21Cso{&{NzE>MRh_}~}$d6@?O@xrlA4r;!Om-v&E@0T5Tpd~2MhvX%A-z9#Qa;bfUV~D0ijy0bG$~{@pmw2<Lct7+fJRioEgI)fa-98?#8`&S9{bppKsCq_IX9~+ksqmn3r23=#YR|F53xX1bsI0VTd1mpBs#z??P#yJlH3^I)+TOHnC;j{U}1K@?UV*r5Q-iwd@85j&M1bR{6$gIFml6mC{}4&si~0aHibi<c8ZJvkkEQXNx$?vqRK(N_>>@81Y)9@eyf~Hm_dwhIF(s0#cNhEDPFTmSC1s(MU801rJ$;!Pj`Y_DLpZ(R=p<9jegz-g|E@bDy;YA{g=Y_e(_in>>%zuPv~x^zyxo@KT6%N5nHIdX-roY6Kq(;y-pQ1U<Wq>>^fqDFIpRQz4(;M3+hCUBbXT~r&D8e;cwcSV&QtBRt)^;Rl0f_6TV}B?O=bm_w(KWJHN!Rb)z9bC9OA~5=2lMa#FUoO(2$2JA+uqNw5XJilGH=r1v$^XasDV%=K<`UwYWWXq;VD_)OQhWngdDe)B0|j0i?+>>Lf{(~$y^w}m7%{Zw@E5My1mTZ<ko94Wh?CN1p5YmSt?AH65@=y$<mXImw*@UatIqw!l5Rw1^L*hr!OcqG=SUSzYknpL)(h8gQ^JBc#Ma1}>IW#j&adcT@0l?G68o=FDh8K+`{3$kp-?6%i<XnDhF*AtULB_nGA>r+1UNN3ZwF`PA>J;YdN>+X|Pi=Jf?y=D~DJk;_O@TRy<W_+8>2u?|-YqUKl*}-S{>#yZg{c^C&kIlRP@~dBGy#M8&|LxsxzRA%4W<;7)q-UI|FbIw?m&2j=q^u~ZOa_p}<R(TeCO7F~Cq_;Ov`v0l)J+{ZYle~BY`D{MllXe*XutR`ps9Zzc%WEB+=O|z!#J8+<BpfHpn+G#Q2{TE+iM_rPPDmrSLcv!;Elvy&Zdr3Uuha@r%@1p1-eEl1O~EY%i8hl<CGHZ2eTo8)u@qXTxsj+GMZBEVnr!;>0-gHs~t=j*Pgc`<4hB<X|-!;8S-8)KAlpcy<pMIj*}yemA^u@#)YhsVShp^o-2R5)PIIjs=HC0>lve+8!6i}yyoZ7O81vyZ2$AXLo42s*m<{fk{A$pWseY~q;17k0k6DpFQ%!TCvBKIi@JrVZX#yOpZao@NxQPf80-%JV!UjB9(aiIhmYT?TbE!A08a{~Hy-V4#aQ#IG%Uc2I@e{E^^68CPF$BcM_qGWeg=N=FUEEG=K+X_eqx*ly$WSJ05EJUmq7#Y5Muzt&T@Ho*471e>8T4ni(cJ{(1PdN<UnzIea)4i^3B&M@r-*IkZ{1OzLAWBK>)}yRVfPBPb-E%R_Tgd%_S35O)PagWoX)^St(-_x5zO*C!9$qRZ3LRk;tH+L*ovbjc&B0@_5;ME1v5BRXWgGAS^vQt6`aC(pgo{!43fWot_rFLU)g32E;MPb*Dp(PyxrqdBt!J1`W-y$<`+TJAQ6r)bVqbuE_O$>2A|tqZoVG$}*D?Hs6k}JLLhSW%m)Fcjy|;o_3c-|2S|#)h7Qvc*-&SPN6%1yR9d|n-f|w2ve`p#a<e3lhr(BmPr?=W^2ns;l|qzm4IukEv<Jz&hohVeCi2GH_@A!S0wJcnOX$5^)uuZa>`Z=5WGqkbvG|oExct}>^8i6@hPa3!JbpwE24D#v@VjJAX__MqiAZ}NM)=wlqmla9)~k1<&+illcwgwk=Lm}6}M4Yui<=%G0w4!5VfoNu=gadd(Rq^G~QEO7|pPYX%W_T46=!!BL&x};8ME&KRqtUB~a-}HuChWnRa71V7o1$#dDjjm>{-kFD2Z^Hh+*>wIIuS>NTHp(}A<AM8YdVAQ9=BygUkR=W8@<a~3RR+%)BUILQOS1(l!_Ytg3%H%f{VxMjw@r^f5Ry^0~YyL44dAW_3!R|qD%yoLP89_&o*KbaA=UA6)&@%39(6my2)V~(q%EvEcoAcBez<xhAXO4&|l9VOmOC|tI16QgAdt@M6LA=XhRToWo=iIhw<y3S>el_C?Ha23W+(h{16aBIhCD@D_9a=uV1Admxjo<R;22Bz=w)GQlWJ6t8+ih+|oq|0)HM2YGvX5`@+lVmL1a!2F^BPi*LM|G>;^cGfcJ|Bh=Jwv-RuT>!OW^8*m+hUVCq-!vShnOISq42WNVr#L}x(zl_U;ASo$!Hw~7gsOQy(>lgi_taOYi1ey&DJ+ab`*sz>tWy~1g@_eQ%7*aIJ8pnQD?|nF{mM3rHh>z?hc7!mQk;uQs+=R1@&t#%yz&!3L=m`Za!b90VpStWYaYTyX7>E+ZxNhnvF&uV%Umj3~$l}2fr2ZTDO6fnz&psv{SHaIpBDRnLDm0u+3(_?R>DI_8y5b#TmAoeuWt}zg1wp2HPRVu=%|W`mS%3WYs6lEHhn_BB^frYj7(hg{$Fe=9VCu5>P#KjTYW=(<qWn?Bn1?4M5h_kEn+?p9+9CCz%_G8rq@Sh`|okt8|g3l>TzfZ)Tb4DuFqfw8OJm&NNz%c2<^Sv<VAxoX5@QQ&B+}dYYTu^Xa@v%<R0+n2G-;MosR!G%P8Gg*|Iaj<bwN$x#X?@=z|y4z5jUaAG1YX|xF-l09xdADo~lL?!&69$w0+8{wpFc7(q%kXr91M$~#YX}qtJ?47!<Cd+*1D7W>RJ%%~fX%|&1w8Cx4PZ+4=ar5~wbO=K$VOGg<>L7-xk7h;;Q_r3_V3?NK2=B|vAckq*5pcz@Fm(?Y#=0Z|R0TS&0}$*Sx1A3+RPJ#x#MvF@(-dahWGCo07`H=A5VuXbTFS_gg7&OZIs&_A6^@l4vBfR8HcJdR+Hu`hH&DSlZayD|sG8%+&2(McDZtPckA|O2S~1qWDqY#nSbG65hOuu-=frWsFyO*#Y{@Eu^+@;XZRf)cRd7Nupwt?IjWceAPSo`zA}wxLG1NWcec#@Fo0$_tEz`&{*JT<k(TupUS<>m-o@y<EqUv$;`7lI<n#dd{c9C+L#&zMmb@N1}v7i+L-MdN`%|2`@BOb)iq|obLS;;nFSk?(G=Pla?W``OJj+;-1p@2dS8xArR+sbKl6Na>Pn715v9fnsi#E`ZIbVCatNKUt(WC#11ed4HxM%j_LBKx_0t5!43H(dFC+~e8?gb<1`xiF)ch=(@~l5OK*?4A%C6dN(HrgC(5E$TKb!K=nJxZajlhuLB%ITKuQOmrj#gbJqU8Vy_PW47@*`;<_)NFIhQDB6@Bj5$Nhr{3_4t>1zaNKMd+K}}9}3QMzS9AII~D!6q)NrSFLKYGeY%egz(zIFr!-FV9PDnS!koUQDvpZPQZ{Y=}WGHl>##TM#k+S#5L7#gQ5=-yR!!;mOW7>0pk3-qhEB~yW(5?rGZaW(%)c-(`Ppb@V8LqF3uaS0pagl;LUiPGBBilO!0-aD3$9n4;jW!Vq0EQd6q+imhvwko!7&L%)~LptTlhoDKW{8yekEX!$-8N<4z!YNkvDu!0JN?BGOR7kh7VkRpaMjvV-?u?CHt8%c|w4SnnDjotC1pyaF=7IXzmrM~7>0iZ+i1c@9PqVMMM|a0P+7Y-Bw%P$X!`43S8@5Q_j_*}WXciaNEe<aF{KNIQrzrt-94>D#^C)FL9rq!S-FJ3(g_;|TS8+fvUZsmEW~LBT8%lMKzR5#k7;R|IW0tfLs~u0bo6o0;5zV1fa9|KUpZXcYFgdyo!$T|(!zx|O!|ol*7{b8|lAp2=s!7bOioTy5fv|aVPS_HH&D;4^xSZNF!<Hr%*bcTsjA7fP_g9f@Wf*k`J<Dug1~-wX5`b%$LSu6pS}({EdBOeW^N|RkGLB0WW(-TkVoVa<mcu7y7`7FI5cDctZG`qAYA^d)7P^;xlaqjPBebVw&lF2b;_JX`6uL$2+eje>F4SYZ&Cx#$T~yx5>C^ncSWZV`JKDz0O;C7uZem37ag+M{8d|2SG|Os|OI?A-q#)zk_QdpMi=J-Rolh-M?IyaA=HN8CIJPp<#zt2dmf^CBRt(g#O6{`O*d%pEJ&U-9ZQkkKu3J-v)Y})17!%ymK3}6K<>S(Pi;6hOnR!NbJ54i~aj(M&1~ZOJxjHbbR2Iv<3&Eg@COhB5DPe>&x#a@rj;@6eVZ?s%`P2~=YO>t|xnlA}aYnEb&5@>#hZw74+REf<;U&u=5f#C@dZ4wVV`s5RwvBXfv>ID&iwq&0uhGVw#l|RsGwNl|=W#it1vMNY3UMX`$>8$nCPSHYtt4d#-bM`Itx{ePhZYTUS}Ro+wXk#@Ii^xVcB(4=XP^h!FFv2DB61$`G6%HfG?^L8uyI?lhUFo~SXQZ9scNuRRn;K2SJi0!q^j~|EA(#Hoewxf%0tOH09<n840HMH<2I`5G~f;~25ytK-Kz+=oW+wCt7e(e*j!4}`+y<kD4bV=9PKvK2)0w)ZayEJ5s_q(bv(0V(X<Y+Ad>dnP%xZ_*g`m~bm20|k!n=4%y#{$<irfjAXK=9R#DITZTYzQd>AH(p_d3S^wuB@nY7|SBPInyre&WFIj`E%EbDD4xhO*-Le8wI>N;jmG^l)Bd_JB*gCG=SRr9Hr2|_Y=>N|}XLbyxg{oEs~h>a?QLE-@6Xh~fy_Je)h)Mbzf;kJLTmNW*=hMLM+bILykkAsmAjMmJ3I{B2QUXBfkk_8eh<Xpv!3OR?A7oFndW7HCmEDK!%LV?ETD)PjT<DA<efa+OC7>{q&3Uby5D<!+8R9-0`1|uUFog+XU>D<V2R*;}=3S3Jxu41f>Rl3qfe-Fhztnq22ZOzW1P5*GaRZG%A!6UugZ$6(r1+^TJz0tZyu<W)PpQ_?k6!pp$*;Wh?Jf!zmg_IAkHr-$dO;3kiy4&MuPy>CbJ>73RA8r=m=EA(1vYa|I<7T&IXI{hY5M$izxJM1*mb4Wwn=JNzFx7{x7PsUGv(tr+dLBJc9oL-?I8>XF80IZmQrKF7!nwkjt1H|g#=!Y~ZSx|4VLg7~A&aPl8C@or+DuSzx4-KMA{`24>dogv5LIMsw)=DJl1~%Q2!_IM!?=gB5nG60*jQB-eW(<S)u!CCOlXK*y0?kZ14d5^3Ki3B#bZ=Q1;5GpkVPe%^5i+|W<H%}05)*gYjYfpSVNZmlKnFWB)W2X=?OPVMQ{pAE&ut=spy)MH>WyOz_GzP0B$}_6S$?n)D>!1Y<RC?Xn1$&;_aCB4b2c-%c+la^2ce%0uCvYIU#m$#|YB>y7Orzs@gaYRx0I4$As~*Ds61YLuu^>+*O=VuDD5uw@O?{^}1GyAGA_}oqQ$--0aV7HOB^2ZriUr-&z4wtMP{1G!~ancS4IExEyAZ)~#_BTLn0~y)rH<;QD5PEbBK5kW3`z#6Qbsf#c@$VTcMf-kLB6JNYyM<@4M2fTUr#iODD!`hB^;eXB_+su-3;pu>>spS{H}M?^{4)!Q0P#6ExhUajDk@wSpxl$}ivWOVeXQsZ-EGkZVp>R)NVvF@$18je-$6dVgB{jxip$m*pUvO~d*rY$W*#x6_WZCi(dRB^}6=hH=0s|kCzZjWK$neisw=4UX5yI8TO`*@96_1)CftxDHMyRwyrS7GDT9HjKbH8}`=D|QXxW^fU<$IaKMLyysNPU$!~m;c!OIM7j{Czhs^dAUoD55`@-X5SWQ;9{&+Jcq3}>4F5%1Se{%P8jx@nNZ)`T6pshv0c3#Q_F3{C=fbsKA++`jA0^xp)*dyFx<y5+{19${TN)ZI)>g37^WG+maJUaZ`RJm;f^_Q&FAoRCVt#2h(`sXtcz(Hdz3;jWM+8NSe5JHJ;WBmn|2D1`*u0j<@UJW%DY}gV~A6i6>DqHs9yt$TC4bO-<(gke*-`8Im@H^Ps%aUtf%Ev5RfcAJ^27+6@%rySE*asTD5I1^dw06E7RE48f;5Tjg{22+q2nQq@?xc^BG#SKP#IQJ4Hy_>9~EF_W3tVWYvm+p>3r1vlWx{8jp!XjmOOOSd+ukRg`Pmlrtg>kDJejVL}*27Jy;2W7VL-K(W`d0mCY`3Wm&!P3Yyr3wpW7grVGH;z(*uwdeqhY1_)mXmI@aR@EuGm_Mt+drtho9UlSIek@rxhdlYzi=9#Q1t!eSc@rac&MN(kR;$w5txxJ$H%#g{H%;nLySzu)wbbp=hSqey`FvX?31OHj|KWgP{<vtyFcu1N{8ZVBu{GVKt1(s+lGnr5)DN<noMVm{2EUvVXmiH=nG=d|%sIQq11S4gHaQKSAS|Z<!ZdCY>>*4$Q#Dq@Lwdh^JSNqcW?64cOZXC}(JP8S#{fV&CnHA?gdwq+O=&)rgfPs3B^%8l9%AeeHyUK)R~S|rCS{z1y*|z{PtwV9y4AFVkI@@E&q1f`xC)@4Q+oMkf|c!bR7u_f7^f8j8Zoe{jr4xDcxS7F8MitZ;o(uluq@YKaho^BaC=W^AgmXk4?$4QvDu`8EF+s#vJ--(|I{dx9jzE5SfsSbGR)f)MWM^xWIjQKEHc+q2IQlVmh7G6+RA&4$~!b~H=hqqkKjz~X8W2?{j89M|28T+UvFZx^YtdZ+r*kbyhKWMZj-guxpKh$R6zOgyN$f*i>N#BHCo#TQSi2=BU|Uf!{7x)9qW^wx1*O+J8G*OfhaDZH)5cza*JZ`Ypc`p$xYVU+j0<PRMFDTzZ_R?Tka$xc=wynr=oB#x}h{%+_1mX4j87bRY1k?5MvC}*k^z-^j62v%Yb1Zo-lNN)n^ylk#o-w!*J5LC&AtJWCbj@7bb)u_o2&(VHH~iL*6#B&|N+BLC4UC0Yl&O%9Rs|{6$6b>usllEuunBu%6ru8z|&l3IE_;j++?m<+w@1!hCu!s(19Bd++FnvUoY7j(!|M7`iT=gd!Y&(z19&g`D67nNMe@_s7caH;^_C7*;V<y<NJBaJ;>Pp&c=FJV6+i*0ZO%HpqI`i_eE3s^gU0VvbSssUH!vTN|TMm{nUbaDZ27S{P5qCa4aOQs^CE?R0G+w5c2Gzz-X3IaQoR4p7V~+jpWs4BgD^SWZV~#~lW*w9;M0u$As6<y9amdY<8A$TOVnAE!19Q(CAYN!O*t$dc|ipASP+yonq^u2@b1hEx5e$M6tasE1V=S3wNlv7|g&(pe9Ec6KX@z}R+1S{R7>v+5x#-^9QH=wX<c9109>Uy7^fVHGDu5C4Da-ep;q9M=;2qPj)^d=4eq9rUCnQ&-DOBmMthh!1xUl2{0y%BrsLn`ynPFOk7K8Eb<CKwz!V9~k4|Hq6<C@Y37<RBVbbM(13wx?m?<Mjung$-XZypV-N$c2f_asQt06MvY~s^*0j3$&ByCVWm@K!$ynw@W-(EBdO%R%zO=o^2%J90ndz|o!^IHf*6t>%xSAJYYcnuvGL3}nK6dxYOea8+{f{Tk1-swZWD%O9YOAKr9Ww{(%b{-0ORpz=l5ZV3N+=Kuir4Fw5yK|7+9=Fjmd0b$o?>n+;QS$V@PSkkiVR&F_1-AHODp{`r-9is~gV8P9jjKQMvrC@i>EeO>i9aD@R||kP_SK<Fi@NHF;U(0I9djrk+<o{dkp_QItaN<dia}{Xr0**UqnV%Hj}$fi*`!TTOe#Ep%nEwC4361Rpbe*cjIGcjgm<WZsQK-g-h%hpG~df?Eu$oE(7d{;btW6^fHq7z!zi{EjN85JItj8AYR5{7#+QfbbXXI67J<GdNmr>-VXT_FZO<7L1Z9t~iqN#CmG_lN9+PhXY@yz%CYnPIR&U#@Cyu3Acvdy-pQS*a`LI586kG;?AYp01B7p7-1j1%>sQiblT)_@KH%cOmx$hjz|f606Q9}K}mI~_rQ|swcGp7RzbK$Aa&GMqu02li$cZl%Cs}AEVvD8IeF!v1Hib6TVFFo_8>>-sM6t9wQ53MIW~SDf*v7Qqb{crEF?)InE%G5O->oN<1mwPI}YpVVMC&rA*;kK!jV|Mgse^8=GZ02vp*&j?0{{o4uHU}#)YN-Z#@Bi26TWxvmSlztKf+36E;T8ZZdo5s%6hUjU(2v4RqlZ=oEHv!$QmXmqe1Djo;_0pvn_CgSnixsv41FmxlLkj1gUulNnr+kM;C^C3Y$zyQ7Zh?e(k;@d)7BhYY=Q?EF3iL5(L!QzughwpFhY98(+kuzk$%u$`>meb8BSW!;w7>(Y%*ff>Vk+8yh3OIji@ioJGzABGuW=$+RgPhH)q0=dg@jC9iQ!rP3!(Dyz$I$W3>l9`Bbh-)IkG48h{b4jQ7D>v785`2RM+gRnE_FU_H4!#}p@psb61=V|ey_d9$w$-HJ?ZO<!sK8^+hKuIwZS#8>Rt8U=k%{n@iKMLR1!3u+!flXq#b;_UTkgqs%IlItB_HnrS5jS#$VW>TxMs^oZ!`AMA^UM^l=DntSMA&i;&@wJt9JREmduwa!{-pNjiu}1UG?XjgKNaj|I0aC*hE)jON~}<hqf(l9Ux7UW6%IV@{f-hk$-%w2OWqeOl?dJRMWy6S-;b5t~D%8>4x<6(nc52u=%y|uUyjv57c>xQ&N6OocFWUh5HRFS>O$GHo!}}Tnhv*h&5SWP0K7~*jccGUl`@_Z0r1(oW(=z^fp#<8Rx&!&tOGTSS{z(ijmg?(*i5?#wus|<YhK|^0FSZTEplh!mu)}unH+$%fCR??ArKsK8aMYveS1ZXWA=xjhT!ar6a@Ef!VApc$YQ)flr!ZpVhP;s9~orukgBmg;R!~GMAg+A)WFfUdtpG$q9ki)N>$H>umN0c<nX%Fq+(zG7a#?Y~lwz#bs8%^|Oq}+%MoFVhaerr7|<YLyS3m|Em_+0OtUB(m1a1{M>BDo;R%N0A99vA*+qn`!FkB$eM<pc_IG*zx-QXNE1NBl#{cbscIbN0Kn8qA(I8*ZN>nmixl$f2x~~DnrAhw_g>nsy|}bD;%jdig>w!%pWem@XK4NFXRyLKtMT};kLUnc+bp9P>^ZX;!aA%cnzB$_GX1i+U@4Y`qFpp+35SH^PlB8BQWr(_9LWsIIkYRYbiU4(%VRC`*{llz!@99XAS@#XZ(&(&EB9{X<Tn6gV;zC50&8<?0kh@zcCSN?Pz5Jst*u)_Lq{4m>F@opW#?l?Ejy3(#6TY@7ZWzhE8T9zC@R=u_6HKOWz2)1pa9zD4^CHU+H^ky7nE@7e+R3KBVHV|8@OG)1Ir7{W)QSK)`MnC^u=5EX0`F6O&^2ft%?)<f(nj%+{|(n0iVvj@%y|JlyP!&n!6)knng2!U7UrYLG(DA0f3M7P=kx&%_3a2rG+9eN-W7_-Hlx41`xtWZC>Oc86CBH8|yKo{sEoTBcuIq&47M~GpOs-Uw)??wJz?gNe!yp$5x|-^KHgB=QiatYuDk+lcIfjlHV>*328rtLoSCHm*;wGF7O@&+gRW-M^C+VQA&=#!xof%iqEW`G0SxpZrFAyRy;{Ln+alDuDZf^Z0i?kl@8*XH!eYLtU2pOj#Yz%BNdN$nS<O<s4)k*dK;^6wkE!w`uP_~{O@1}1)`EiEog-~OUVl6HhI3%&n+-NW(f0TJ=GZM*JI`~By0Q|DDIrR!Hu314d_gTUNSh7pO0D~rc@m}B`>M|gZx7jL8XY=xRYxA>8sjZiTh~^bqhXbq+2jszn2>Fm{r2HqGf_$%}H}ust9`p$>k|Mg^`q`sh=c838;dk{w42=suBV@QDhBrqO=icw^wKB1n5v(dp3jn_O>1i50WLDkDGa=M5`&a>;+w3ShQd;CA8Fh9Xg1wo!^IHMz7PMXq^J0==Mf-*EK@MRIS4p-e!UrrbdcJ2SwjtH6C;mIm=I(ck0(bX>O;)kJan78qnJc*yayp)H?gh&wwrn=r}Y+cc9Zgk20__=fY+)s4P9!gCIrt!e3ZzZgo}}L#2ze#v=#iLZ_8k5WLsU?`uW?MJAGZjHwv7t(INruqnZGu6l^!Y}OcltOug|nk7W5A)(s*^7`pj;OjZz;D}{z-<!ycKe%1Hy$?3j0VFw>tYNEJS<<k@UYCs)Y_}Q17Oy_&ulJ-}V|xfTDT8w8flbPg&6{wm0eU)3O0z64XBp(BXd5dptub1wQud$acQpq_EkZtwZ)iogt_nc6rk<VS45^P!W}rU$SPv;n>o2#iP!TFic~GjCZZB&snJ6csLO(}AjPA{Q=l6N2APghvKKHt+CA!>2w~T4;KW5bDep%Cz+*dfVr43wdj*Kv<u@P@Yq<X)3Iprv`)N?8k1hx0h?}HPRil}7*>NTdVh8<4ovm^X>2g&a~W<-AXvF7hC$uXFcFx9qp33au~6~l`7tnZ9nvdk}POu!}Yo!^IHKp3{dhQ`oK3J}Ax1~WT`W#u>AFf7N!gfA^Hh+*}O#Sgg*zL6QjJmMHoAXu8&!Bfk<+xu`sm7kDP*4JTOElJ}h7g4suxZP%gxOMAE?T|Ai>#?QU!t&U{c|<=mZYhWyUdwoHQgF!wUBi3l_hE<%J2iRLR9k%&VCdV8h96R9Gxoh<J)NI5QqCrZIp@{)I4cYjF2b&Clc+X#@Af|2P%$SZ2db_q`K)nkoS$K(2AOet%+U9UFGaiWXQqg${8DXxNi=GD?snYR9ck_TI<A)xhS$#T!w}VJGILhEA#Jtn>f78~e6rDPFq?tzJ=Q}95WA91-@DX#KNvRUvkk*0-*cJ;DzZoufL4-w=htB<piaY%lc-u-Tg^dXNWBStW~f^*e9RC->OJa?4tF30`}`!oou3rd9q4ci5HGW`<KMC4=DxQ5{JTdhZ5Ro`m`g|+#zMTkZ;*6l!MwaDPB2bpaGc8B^L41(u%u|7rfEA&$Bj9ISd;&F=q70NHB-kdDwv{etf9Pi@RsYRFTcYURBc*ohE5UdYILlzjXg?%+yt{3)a2x1pmc|R0v7gcg3GHubHyOX{i@7#9L<|$)N(Y?&98jB5;Vod^_bQESyvPA&(f!tVPDs5_Rv4e#ms!b&;{E;omdK6EJRtsIa6uHBe73rcx!@f3}<!h>zOfrwA$V8kVYe4)de(dQ(Yae04vh1$|gql&uoS#aew<bK724oJFw-r1-6`uf|nzmMu);&K_7vZf;W{{zJdsv{Hp)Sx(TzbCeauUQHfxjxQ`j0xWmeFx=n??oVZ1uxM{yn$Eya?Fj$6$JU(V$SxuD)fy;t`%R4#2`0iWU2np~XGa~~0%evCz8&_rc0-Wr??XZ!{e@R<w`Yb#{=|65&-zKY-L-}0;fPkV-OSrX;L0jE*49H(P*-O>t1LI>(2*$^Hn0jVP**rtrH@GGxLy>0&vL-cr6x5m%OUf{@96P_y6*HP!r-{sFS&bUQq%OJz!`mzn!(lzl&+?e7!Vm`8(n^()&`4+i$=&RVjv;K(y%V;AV2fTq5w}%eG;BqYy)LlbW(-@mewPsGH5qegotuE!0-VdN>H%R%B@F%7`8ZAJ7QA+TAL#(9>4ZW_V^~-7o-l@<&lh8Ob~b|$^s$~Knd4w}h5D~C#l`tta=Y`j7sXTm6wi`d*&|hLu!~jdN#bK0^iQGj<mTU@i%LCNNWV6=)t%^$`gpoa22Ib$j3_}q)_BsrkIvS;4|&tQPoi}1*<io-%TP2461z|GPPasLoE%co!D+5v5o_Rl=Te3$6tfw)<*@o=53(!fOuH+auy)b9+Ot5KQKECJOd-_O*4r3P$!Y%kcf461m3L|$rDtmUYSGZ<t3o3f+Pq6^Ezl0D92B=38KH^opzNQ(m9&Ff!cn7{=Xlj2@~ms)_c<pj+tgDGWR4{gWsTrqrX$NaZ!>nz(hKP2aA%f5(~A*|iMLaA?>TnG&v6fqUCZoyiI9JSZLG2Ai9sv#??@*+DCkJdbF*d>so3&<XQnn8I|$AYx|128JFI*l7Htj|tR|~!3fAa=%W=Ovv&4S^x5%~e`#cpf`%n}e-L}=@G?u9oW3h$hZN^v*YdCpo^4|2+#D4YE?4$Bj!ST;ZJOQuIYrFRWhZudRgaCl6F(PT;avyCC7I3#21J|u{Un>Hxru1Y5t|YNo<S(roe#4MzNY1AWWu|}0Lt15n^WOP=aAw4?Mbm%8vI@@BawUp_;k?Zr!a1ym8<PT=hibLS(z!}O+(slqjhg5b^qe1^@15U=VSyM<Z>#gFcL+l!t5PtT$-t25*ug^~nrCUXt!F8PEL&F6G|o;ucRW`C92#f7H+~;Up+gV~I#ugx6a^u<FU6k83?aO%`FrX?!4kdiHqIM_`$-x`<^LZSy`Mg7M5t|G=>FS>@VgW;5W>+r-N92=Np*R2C|VXsu&nc#8I^TzD<7K3Ddub?Ak`L^1cd6*xjkM{B!-OWyy!H6pyv6c#U|i<D%X!d#J_`)5sV=aAfBwNMZ-vaR@}-o9y4~wVLiEH{D$I!VJti6YrP!i><<@t=D>#{ny<Qde&2ZtN;x7|3-$_NT~$xWxmQS|p%puu0f4vlyN*cv@&@%2M$mCwF{FKRl?ET>Uph~(-QI_rN4V9CN2mC8H8_o%?}}&9!tFL=-25)425~Ev{!6!|)%VJH<(hFTfiRg}p3l#OUaRiy-Ul42%}7pKJWCqYD{#2b=yq*~yUiH5c!@I~0vNXAm(AU(Z>dbM%m<?2Ie@STqXiVojGf<yAgaiCN>PU{b+xcYFf|e!#?6hB*+T?Vr(QWUs8S6MtogTUTWRW><b^Y7b6TWB2cDHB<tT*8sNi4nK4ekJ#&gh=D{WOx2Xt^aS`!|V*+Q1%ku0<ZB(`%#nKQc;<R}VQ^8C+tt)gw--nAM~F~=t#0MK={D4<*8X=tpl_z-@~@DRSNheu_))R`gBp7VMP`!Q0m6m!VZ@>#tVVR~)%J`Y6|oDhK=Q;mAYi`)!c=|sv~ne7AIV=gFd?AGns5*JmY?V%D^t{jr&)&0o{PXx#eoS+`#d%O4P6QH_HbYr4-1+GT)PBd^e<Rq(&<1u>!INu)}kJWHnqM+JViGrFu?EL4%{}PFUd*}CIh-x;`yQBl3x|)IW#V$ioEDXDuoPlAyR0_VInpCoh;YFSO!H~zFea5h9XyfM8Mf?R;ZSK!n?Hjp<w0``Q^t<Qbh>A9`MmhTSi(&jHZ8#47D{tXA%)!91vDqKn#)-ULwP{o@3y!4ZUWlAO%Du$d86ltCz4QBg5!G!HlkPXJtC|<5d<JVUhL_nmPp`QfcWS{IwJ@WBL#$BMmF%LKkF;jFDIdY_#&LS^Pd>rEIlYZJ85tv|M$9cfmmhMsMpX34tv^Mp#1x-)(O-9|4mzl?HJf$V`mr9E0$mDXcI~9F$~3t&-ri<->lf`$Oo2RC4+sT9_s;Kg+<-AG1TYLPSQw`77^ZI+9@`>QNZ!UU_zlCNF?4hgq@abo)uBKLZSx0Yp24TzJ%~h99IC!p)I4Ne&Ezz^Wv|e!@ZM$*;Vl=@$M<tN$7s4}j(&B~oU-b;VoN`5E~qEym-lBaASFgBiL%rX>xU0p5tV}Koh8~$+o~WOSyn#t4aZ>yy1$P#oQ_(vgD$NPTlJqTdxLB8b5|VpQJOWkDR|_gW9Rpsvq*WXo+w@gNPTq|Zl-H44zpd&W^m3<*6+!RD|U&;<*mfybZOSr==2oi&K*52Aq?-G--lsA7-klLVfK5`V8B3m^|B4aVfF@w%m)Sa_T?qJ+T+qz?WtMDUZxv02XL0oU9B+2xj$+hrGZ^&(z*42W8Lm|(uSb+<H-jd^3>HRUc(q0PME{<V@4dF!}<qpSFOw29^vtR8{r8-jquPc{A0*8E!w~EIK6g$pFAZY3}?3Ic{Py6FgKoY{A@X!@p0O%r}@?zL`FMoEi2E1%T9HYr+6F~SgSx#zwqAqeHenek7wtzYS!ti0K>AY5Uv=Oi_aSS;cfliN?sFevu1D+3r-jom3rhFJN_%C0m?hMI+7p^>u;q#OS#P_Aq;D1$<DeGZ!^9UJ6p1gCkzK!lQG*t(Vp$lGj?*Edv!A}WcJu4BfYZyDuIfQZ|u$5CEM!mlH5a>U{wPqGdNZ!>-Ti=i}r)twIAG2S(+haISOX+oH*9(6!6bq8@~@hP|xx8NCkODJyOXl1Y0kvGsZk-Geodi>5yl*|0r1uR^4t31v+H2g_TU8E~RoL`J_0P-Uk!rg63!E_rVzvoP~ovU+Zer4%ujOv!NIJF(bX$kM(OqYyI*H8RfZdo6B=;f!7g%_T`td^&kJ^q#k%1gKDRwrU24^7Wd1tP6A~eAB)#I>uuHV-YRz_iYw@o8F;IF!r1TL8mx@6+vfAOEo9kUH29fZkdGDg!n*bKv-A606l$tFD*D7tj`!zF?)-p6UogDQ7{jtR8ej~gw=s;oVVH;&h9RDs?(#f-9tgSUwe$NhM3tRXuSd}sk}x3*c@15542Rhp7;+bGp|5@zlZ|0a8-{UZoNFtR`7c$;pWR*uTSk?f)Ya1zY@m|!Pys^uj*l71cYLhraD2un=It0Gt+!*`3gc~wW+ENj43c@7OGXh+d}(1kqe@N+f{xGN)dS?<{oCjqHw=dvy540yML5xa!O-s*238QJ)9FbI8#c%qpN-##AgbfE+(O5wnJDfE`rgOr49?Zr3@*UOS`LS2V3*7bkaAitz|rbDL+Hywor9k?=6dS%vNK<R;$GRmlLcZJH0NVm-8mmG7^2Zj_n2WX-N(wOKve8JBgmF#1iwB{bqq^6x#4ow20|z4we$NhM8%uT8RUv>6=109FDr((*+V}Z)_e+L_{&Mk`$?+(FuGYo7n28;xhO6$5RGs3LsY)WjbBS+m=vu;gCSfpx`rPPb20od^e4u6xD9hQAsiFRQ^ltEVzkcnstb0)C7Ova8`K-W4@*?LsRvNh{@7Nd#<G+D8;Rj$#`of|(kZfGqsM&sW3WnbQAp+a)gO;qEpw3uJUM=Lejl6(!by5ar>(}U;p~0L#*^b@#&D*q(dv7iAIF0}hI159CJf6un%qN7e-dXUp4*fGE%ImQ_hE=?HKl?DT$0kRS2kc^vEDZ(vxOo1!<cf%iIa_?OB;s#<$#UBAoAq-Q$e5~UZ1t7JA8Hc@gsDe`k!G6YB#}g&@Uc+RbxtQ>y^)DL09Hwl>?^UE}{B)g1P<QqjPA6QOZo*X;8DyeN}^%eMj`#`E_1d96~U#CJ$(<X|KM8E=HEt9RGvhV}_3$!&?5%d_s`Sd(xAv2<os^qEUQHi^{y9Q0M-vH4fmXzsv8&cnTpD>z7e9ipB2)yA23`k&~mdbuxpq^|pQ=0cqc5=4inv*=?Rk$`iTFaVHipFSBY6BSgV27PC(MRg!Z3jKAYYDxk&_h(gdVN)&gB-G)!NM9K)e=xrA0qM>suhl7hsDq@10W~JOUmX#9rSavj4gF@@V^Z^U4*KY4SWd%VPfizNEjb1~TE|wL;KhsXavOqYj<>a42=0z!7);A83J>C&Is(QFp>e`|3b!_}T1U*8qkY=}PtCBQ=`EOj>K$US%4l^0|<glKeJ0yx3y-Hj<_*XDv`4Y%V>tQ*y(M>UJ&Jo45U>l1YLfX4as4h;zza%#=0fNqYI<~KZleZ7y7*)N=?4jG1Jt;L#UdJ}*TTN)l>@MhrkWLLCCxNAB<M+8Or~?JgV0Ndis$S*THRgSrbVPUOWCnNVV?Dia34b|v<96)!2k`aW4e<!zLNZvOdybvohaf2Z1Zm1<3c<GOHG*TZ10TGP86Lco^}7!`i`}f-(t35e(MvF6=z>3%@0K%~VHei5^ZPK&2t)6@4u9(E&Me4X!eb<vh8Nyu?1jE}($V4a<dDqtgh#Ob!H{FzuSs5#4>E`&=Hfbhr3Bkp91=Bxk{~6sw70Y0b<!171@iS?(k|LolZLm;co@S3k2xDIny=T-@6}luw0cH@vQ-fzWmQE8Nsf(@vP^`)V>Z20rk5Nl1l1@^(<#-}h<vnkp=`E%^fqH39U>v8Ryxllb|t86r65+3Y~w9Er6}_`OYoHhY-6c_q)?BQ)LH#g{$K8)-b7bpOO1tZhqf(lJt9rQV=x6kevpqD@q>J<2ib^vj9I#N)wE391@E@m*HH79qlEVCyxAjymuur+xuyvoDE|;oOsAj4yq~Qu+)rD{0&keJ0bbhGT_AWttm*S=5=*}&?4()2&ySb}-^c8pnHb`zx3MzKkZ0-F@H32&idLJ~)zXuhG{)5Xu$<+fm)UU8%X$!f4Wsc0V@zemRY(C_j`C;=0dqtQ%-6=Rb5JCpm7TsgIn!R+YfNU`C?FYj8q8*0+Pkdr4;<7~4GqH!C-8RK^NPItS9oRkDeJij9uhn+;<es#k(>~CO?d~hyv}BCfY)BE52MUs^eIE|;(DMe>a+T#r!)203@^SJf6IJkVuuKJ`2JTdf&<P0?4;3S<7vCuj2&-S(*e6|(@3_m+v<8*(MZ;m^h_i92l(aR(ny*BB4(YO^#oR<KL-G&&LNpB0B<t}FkKvyUk6@8GF3ra)op<<?YCac{4Wp%McWv&b<1;f>rng^u5c`Cyo&4-Jpk7>`X~n7&TNLb4(o~5EEJba$1G05n%{juF4{+Pp8s+GSA(1KQXfUd9mx!8JG6_sbbim4*ke)j*{rJq!@7}7AS@#XZ(&(&ZaHKlC%*w08%qi7CYW=^5)v3izqflGYJ|!;A!}LQ8X7v%uu1O;fL%QwGwSMjtS9#RNV%A>QC`E+c=7cvu*K|;tYvfZ>3|M8+T{;U*DU3C(1L1C{m)>PZ^VnSc7wL-eqdpO*$hI~$9hnEiN1L2+N?IWNZiNZcq`~czkq_n9$utg(am}9{66OdC7m3dX8OpN+|i6+*L9(2@IuaJfZ$_2)Z?OfvmjS(VTpXA#F9*s-pFNc3?Y2d=2-)V(Lt+svASc$1uv!lkFPPu&rk+Mox<XjpVq~lVW|O?`-p3_P`=F=<=iHMX3aZ%b5gW#PU7w6l#uo#IOKAOaCuhN$_ZYgU>gfta;=YqQ^@tei@%F-2^4)w{e!6&&2pWE8@OG>6^~ucW`f|BtE%uF-1<$Lg@e`vV3cKJjea*Ktr{X6d3zRZyc|ExiF1gnx3L;)>%k}G`dK*s4rfpwYQ4fLm8pv*E1cV$`pQ<f;QW{&&X@I6V<0iZ-)dXK-#~Gv@C|PCpJ+r=-fBlXp7$p$5L3z$Yg!Op3EutiBnE||8X0CideT?5yAt=~6sjD2%t+;6wtlZP<T1?zVayA4>ss{7VIi^G3rH?c$tjGaWF}2Zo&qW$sej4)f~tf-P83;#oG5K{+wB2bI$b(c<(|#p!o97>vV)8<1N_dJiY0?kb8Fd4yS%V(!LUkJm7t7#_}ck>aAx#o9f}q>Xy?n`_3pa3$e6};7|z>F5YE)d)aalJ9ISSPW?C3tS2US-&e%XTqB#|j)*BYdt)p%JK!D9Rc8i|@T@cW52$k+Yr+s#1U}esb&1O(ydaMT}i|~c_usUKk{Z_^>>f*QYNMkvV+1E3H5YpGq@9S0og(s4F@TthSt(ILlv8l;)!g~nkY}RmotOvGyFA}Ye1>eMtXAHdxhdn18F>#pt`xum)uFc+u8tM>|oJ-cI)m$xU)M78yMhmsuj8ThMpY+$uQ;IiVLTkF87EWg=gIR5Ytp@GsG+@m^)=IsG5N%^+tErJEChsVu;Q!Z;klFB}9wL8{&#my*RRQqUgtXJ7AvM#<4Ae{?>miS64d~WwDne-~8aBz$?YXVyB3zDir1Qz@&=q>`{5}^IgkdDz2;Vf(l+2gA7@IM5{>O|O;V)}C@&gM;wzP(YYZ_xbt2IL7t+rI}w|T4@XD%@;+TS2^xp#gaoS=Y2Ek95%G;KBPa8e&G;lDe`pZ75%{=APhe|Jfa!PJ$hHd*e;u6Vg(SVNsi2bbjeZ8VQ!|Ayhc^ZPIi2tzMPyJTAp5W}(tGdqT5rAXW`EXN#$FFi4c;VsYbU|6!>G84{2*a4Ic=3ocCzuddM4>wc~Y8~48I;^WDY24)E+IASX+e{F*Zapa`a;9Xxz*L(oFEE@3XXA`p3L=L$G!9|z>|^lma_{^;3{kl!QqXK&eHCEn+l_|bS7tNzy<t6_pO;Dn#xSqKTwFLS3=@n}JuQTlsA6~T_CDNDT_+_6YO(c3&l<Nz8yZ$dkr}tg41JIIQpNjzX4d@B$~3Btt)~-~>~;v*9ZB;2D#DJ$fbN~&2PdlDWag}xN7`!H#k{$90A-^cVKxIde5{8CA$BF3Zs>|%-LNUCZ5TE+pi^eC444qS4zHbGhoOLy4LeSvDsXKz2ZbT^9`%{;Zo%*|Lky|6usb?jhZO9iv?e?ohEZLI4tEIg%6^|w`02jtjsCmGFl`tK!I(=(8pcArJ%EsOrNX?tH%>54W^l5~-4b@F;jpA=9;}Ji!*pDpGl(K{w=?dj%`-=7MzvG4jWxjM^I*>4HeL0*XJ`QhoYuUdv&Fg^9cyf34_F|N!E6TgIl1^O-J#)t>&3_>xV*RTOh8MH`>i>b3XJEFS@R%!1L*w9$1OqgUXAKRyJ}rcz*S2hdWJ1vv)MyeEf?kU0ZtceUv;K*7Yk8VaL&A3cnoFMcTOcEIKehXucjBO$4O{xprv)5{|;$199F$SGe6bU@i?#|-D+=QbPCO8cqsQ5k>hg+bF{-<9*w0RcsUgXFGo5}8Ks%IbnsLtcvE@h<A|UcvHG8^`!(BY5{=<ds|d#7`<UV3JFG0Hi&f~$;k#e0lGA>*x{Uj5$uE=B9v`!>tg%Xjz-2+e<(<=DeD^Jxgv|PnnGv)8WnFpqjaxH(0giUycG$>aBKEu{l_(#%KWm(!ls|t<4K;oTBPjK7d#2Gtnss#-NFaYna4)r-4~&mFAs8R)VLqBEW%CTJhu?aJ)*y91Lu-C>;-;XvpqC5MfxdTspDSiG;ZBnX`LY@{hDlv?3x>B@Acn(wn6%|FfrTO53gtEYQ3#IQ22hHu?HsT)5ZyarD+spe^}}&n^+m&06m#qX+ik|Mb?bL+l3t!M$J?uIB`2r}F{{!*SURh4bh=0$z!KfH^ZV!tP<ba5N*cqun)lo>^who>bGEY?grJZ0r1cyJp(_-Yjma;M|1P=R`Pw_<sjG@-Ez*o`M6`?5+EHSS89a5C<@h_|5RZyJ`D&Ng*e8T`dOYfb>@FGPJ|8pU4*6K)cL#MPI_qF{CB&sey`Gk`i?J6rsPyB~p{~_sKij>}D^W!!hg5WInnT{(9;wsXHDYEcX)&9DR}QN`wkx}0PQPDz_Y-VduRF!#`1|sN98g4qwlM<M6x(>!=jZRRMdh6moSwezt3_j*uL_M|Z1b+!wO~7}a**<F9EPS-^2|Hh79{QHwvmoQyz1v`H6vz<YvcEMCo0?2V+>@DB@$(g;2^Xk%R6r~_Ri94>E&=|HUSb7!68^UAFF#Oa=p3cxCfVQ7IqOJjfl4~95k}4Cw2^h5=Z{wr3nf;LX@&59;w*!ercvQL_2uV5W15YpgXL5;3I7g7Ls}P2609wog3uai>FHb7toMg8^6y}5eX1Q(a~*NEly*ZI)xTnSl(ug<*<g6r;Y*u_SEG3>Z#dB<*AbYl78@KyY~Ty$bYDW0DvoDACPX}eePqh!2<3!W8k`V?si4M)s&vB$d#ncsNgq{)+E1SNHr$sQ-;E`)DR2k$GUfZADkJHanTT)+1R*9sV6o>%9SVzhVwRi2<NaKZcGYf9;($wOFt`xF*`eB*q)M3LC^X9`QG_`7#4`(G-@`ldWSG%vML3WnG6h>j?Fw2qIs558yH_P%(A5=)B^59)Wf_Gh!%+9*!g{Yh7MsU=vb|*Q51&czEpcAGsN(+=I^;j^+KOb43o<nhWk+(M(JPsUYM`L7$9!fXN?fG_#`oVrlxkr^%wtVI1<9qJKfDwS4nkzbSPRHNU*T;m>CszZYv*p%P9`cS%_o%78e}r*}1)5Q6w%{$7>EqQCRbQ)Tqz-Q2#S(?A{0va{1xtRbYf;O@RPQ<;|qK;z)h6+{!f`Gxo<}J^5q&0%KaeF_xVmw%!kO0*DigSr?kTKt#(+8Dc>-rwxgM45O~9SLEEQq|p$coy`Ej+xlIBq<wjVC<;U9TwF1veWaBJd*xp`QLo+Jhnq*Z)vHHm`E@lojhpYfY0<*%He=lUF3ARQE0%Ihw}sXB%6RCSaVvrF2)Ub2(10GS?(N<O94gO9PFi+L8Zj(zxX{RXZHK$f7`S+;Js(0Cwj&tcg3v@N87y-$D0nI$%rzN^;I;Gn5JW{8Pbuo)rLGp%2&TrF!??e3GJA+%>P#(%c2!EwTZ3^;Dv!3{WpSwe2F&xEBjGN9xx|)HB^CTj-iIuz+IS9{mZhz#S%D4?M{C|=GF!-UJTi;cfW&sr$R=Ays}HIG)$>2!xQe!Ud*f<A^&Fpk06?dU5OM*!^`3@CA&U>;#|#hQ%X)ZNrc3P^0`159)(2fbC?b}64jDz9wMGD)_1f-z9*T-MAp#+%8uhreKV6m1y1Z4|KEOTZf)dAW-CiznT{YSsDj~07k0h_|PfmC!U=gz<)Mk8d_dYcPRJn=uKS_<!)m3()fvX`WS*;w8*&D$5{^ocrh}(h&?J+;Bz$vnH;(v*t!M*eQFho_G=v~r*PhHKx`C=C>C>Dm@OwPbCUYZ8qPfaS>#PH%m|6s`D&pu;VqoacRQ)zuh1i;;&wOTmx=kL-E=br(I$~LivIr{pGVf;63Ko03FZvi>X!2q&x-yd7Ysh)3U-KLKs&Onk<hoQBr5l1(9p4)(=bN9~gb4FCSNld!uxUOo(obnm2!5Cg<<3PP;Z``R3XC(PX69;AMoxfxk&4i?N%uNXieml-n^IYP<=<Qf<V+~3M@lvjz{6p65h^juh^{8l3nHpj5dcp1zEObzhYc}h+^<zEI3Az--?A)pM<BD_|UN^&Ae~JBxL(tFMNCibg_s;Kg+<-AG1Tee=J`jfKJBH~ShR5E>6q4D~QStQ~hDBpICys`!+I;xp20F0KA25D~l72VL98q<s`eIQtk##kb)9{wPM7P3wn>~cLTofSR&*dDW>7qIE)^wG!>bhb}ZjF{f0@(7(U)ty@koAD|A*NFQKc0Vh)S_BYsUebf)3z!IN0ycTe8X{=fn@Mw4X2~l?4nC+eW3o6Wp8p#ex|*~J}UD}auJVwbnN`Ta~A1z)f2_50I9F;n$L92#$g_;*$mFv$@)E8amB9jG%sC(T&HWZu12S)Aa@a|4G6<~=l5Y)5QdorU|3^$K^RhAy==p9n7x4^^Fe&QeR;_)_qe!~d+J5E*Xc&l0i2~Xc?*nj?vGkWsl=Kng_NcK4us!bi3!wxd<)^!QCe4{c#UFgFky1fj~S7B4(lIuUbRMVdyL2PHpUZz8snjP64Fq0T3FNJX?pGaJ~2x|7#1(AF}#TvR}6FGBFE2`vl*YJ-Fg~vb*V%<YAx%-rK>6Cj$yqtk23>v(CT7>&E)Q#-Q)?BeLOp#Rku!G1sIlHvT(()Tuj&44{z)DZt`5r)70g>o~A8+$!aibE-U=?fq?Q(x{f3WLt>}T(z+@IVOS$ecGit}oAHg<X_Q?&VK}IpFowQZ<T^Fr(Q|fk78IBXBQqzMbVPb(`&9x39p7l3wM(|u-6gq4Fu^JYOlEMbPS)?);un1g_eCGV32zT1j^!w|wXp4E?*V7swekDF1eG0MPgjs})YFx`0<$%yI%DW#HbXF*l@1w)`;U^vVBYOEDG<zN3oEriT}tK1{z<dcWpPOjF@JV`ADj`vSvcsUwys9)n~h308#=QeGt!y;Sid&6)-SJ+QK;*-xlq>@cpVsMUw&Es`b!iacpKvpOkxrrOB0%C@Akjz0ZE|1Q~zLI3$(XYzk93PK`E}KPiEk)@`-4_duy<A%Vvwl(c5fUc5e-y+;OIPPC1<}$$oZzpSMC;_SR^mb=e*7Z<^eR1Bt%Cd7CkuWp6*ga7O<H&UrMXV^6Fw4Dry4M{efo<$}ER+WCDLqH<5FH>GF{Nth6ZyaqHohQsU)47uyP&{vm?$;L3I4a2xn(X|!H{Fh4S&u*`SEu;EQ@{xju4b*oYjzFmP@i8N{kB>DSPSO~~yeMNR>qQy2YI$2C?++Jc{o^bH%P821FRhkmRNqNK(D50(dX*f6fE&-_hT$+n*SoBzU?=)77|M=eU<F}1o}MdXc7k3QUU_(CRNZN5h7MQjYTOa@y}QvFWUR9pT!4?Y91hRGE}0h~#k5|4qXBk?&@Z!dxb<VsVVXoYx&Xz!vZ*Hv#4u<w$hNwZL0&LKqr>ho!w$QTl}|CL*eOR4@n%fw*XOGaXDKI-oQcf}7YL_+=krxY)t$^4WRPtYz^Mu}E1b94Lzf)Ze2QoI%b`m9p{iXnE{dMmC5dO3gfVG+t4pGyPEMSFOD0AA&|nCc7O&xw!(0rP4E+f*9xle5O$hTEkmllx(OK6^G1v!}&v=*@>W$wAB`WCD^CxO|Y^za2*`)%E6mc@+8**6b6yvbbgFf6bSf^_gQW<|K`KatNS8hmP$U1g@ADjuoNqUH<t;Ve3>}|@%ljLN^aHgvd>wCr@$9q49Gt+jumUVQw2b=yR>$)(}Awq%A&hNtzRc%TI3%DeuUFdAUz+%0BOlAv1_J=X%juR(4<IicskiQ(TF&Jc@e*TmY^uz13MqxGAsGjop+51L+hAF7x1jj)?gY;F6DY312KAQ#In3q)!n0mXe>Us4_KN^c>7^Tc_$Oe7uLSyF;qSwx^^UC57f`K);KwC|F9WHd~vb1LV9|RvWeEAsG@^|JFf@I#4p4Pk3>!?+tQHCpX0U#1OOz+QH(*pVP#*z7lOs$0wjP=_n8ph&x7T$)0zX;6H;X0YY;d)!YkBGD{Gjp_Xl#6iW10*R|tmmdbxS~*Uo3zdjDcHs0-YJ3W^&~mBk+2`WUlmaA2}B`iCnbtI4{rk~T%u-#o%A*fbkfinn8U$IB^9yJotZvYkChVkXm)g7gQDxg1p<q%*KY4SX9YnRfqqh3jb1~TE*=)cLDNptvOqYj<>a73=2a=2*7Q<jk9dU6(jTizc1GoavGMy5^a#O1ngFY<O411Czj0|3SH?{_%w*h@!+Lu9kSJ#SDxoCP31j&Z%1Uc(IW^i%QEiS|UI}kwu}AnA+j}mh{xI>wlQ%%nS&z^5RdDk5DIKG%H<>+j+p?#n#>wm01bvMEOh~7&qZ<~ofxo1}^lbb-w*{4;z!?npv{lv39J@TdZ=;at?wriv?tHAL*Dm2N=dRz5-ToN9p1UC)0i3VSl%RW#o!^HbDE<U#@@ERcw(2#4WAp<bypI_kyp#324?2tgtlPqRbzZuGGlnksQvuH{omUPvXI(qL55tTw^v>(>r>^eYgWQEaMw)4O;cdoV=zD`59WGA}$;?kUNVG0bj&Z*+nfJREJYLRqAP*le!8R7*L}T}1pNX9`K8~LOjVeOE-cZ_E+iKE4chwJL@Zd3L!&&q7Ci=ZnD}#d1xG;-s7iLx!g^=XfNx4YEM0nt)cdGi5Lq#F)0rgT{jmTR|m)K^@TW>S=)}bkKYOwQ6VpoNdnQwYl;cdz1yd}<gi{R8#ZDZ+&)Jwf$KYq4vbw~PtZ;<`Z|Ksof^Z)+O8*`UJ2OZ-5-~aP}s|f%7|NB4xumAk-|KI=k&;R~E{@dUG+yC?5{{H{&kdq0y2Amp=-;Q@%+}cT+49H*_fczpKGvXKdSP#+@H4Ct`@tS7+Jd6;$+h%9$*<Fq@+~-N5YvVum*MB>TpiD$O1$&>xyq~--+>c<%LUEY0fnwUFVjzk^tTFa528li>?vz`>mxayVXb74oN)XWm+Qz8=CGt(l?Ed)zH${3}J{6!nnn?ppy>ZJ~ZhDywH@&O}`PeWzl`z07*+d1L6vE}C@N*1;pN-$^rbv@3JAJuwro9l@n9;aVN;2$Bn9aHncv<5gxTz^b8p6uu9KuecUU7E+3O^117>oM99YrLjUc_rH=OQ^F6q~9Kq>r7=-axTEbsuJ=LnLcNx|$Xx(x5oi>eqji7nwC=>X!|#%zuzl?ca_fqVVB6tX@P7oC7FIW8lV9e6tz*;;^OzirHqWthSY{AY{c<S##PmQ{^83ng2ni%6~hOh{PvnJ>u1<)&Y{K^HnAb$=i&POc!6}*WucbOcm0QR#v>BVlbl^=iI`$8rK>vL3h;K7{O2YJ)&^TYrL%N6G8ydHdQGGr_XGLhz{$CJ1rEKOy6u}KVw`v6z!%tOE{z`e-itYx4S8-4@qWF9->`Drn8B*+5nbepUt|kFsvJ=1p+m4@D`}mCd+{vIr$CAcu~)S=j-K-&4Hlj-tN8d5h@LZtQCQ42<b@UC%w}Eb_;#Xs9Wf<o>=o^>xr}Qqnua#nmEahUz{h|gYld)v-=3}M6``{)GnLl_yUwcJ*cTss}!X!?&L8pw=!&-)5F2-WCp?Qu!cp@3wtR=+e^nGIC-grw4)qyIb5g<BV{4zuwWYl<y?1r{vW;IdL>XUDn7G%k}1A;(I9uJN<2e0n+bwkuKeaZ$n_hwQkZI6S)O8zlQ)K-b_btGNJM1mNQvlD)!SGg*N9x&=v%3=d674u3~ELtkCb#pIZMe3<u-D=(s3;)KW2#XWj$p$B<6)18lBw|I#JvyN`o8E=NekS1WW>uy+3NgS!0u!>y4g$z4~8(Ca4`z{i<~neP7jHOWaRaNay}BBRcol`aRFdV=e<{tWE30h>CMqBHTTXb$PldVIC_pk77!n$K^SXRsWI~2U`K5ohY(KJ5kyQhTA)ubb@H8MLe6qMSNS2Ign(@=F3dx?P-Q&FEsH&Ed-y+Wl3#^R+uX5kDXr(W<+mnp=d&PqUiP}Uf0DG#&n^>VBTheV5UYHMF%0&V7;k!(^{fn-ia{-rI1h0Z0mK4BvH{e)(cIWSF8tfl6r?UI-lR8oDj-!$TIFIr@aw>0A)_Z%w~|`KGp+mfB3@kR&BJptc-cO#bx7}sc#+;&4MUx@10-lvY-M*QcpD%BDdADi^MhIjLz*1!JN$+%#Zaz4DLmu^``1&<UJ3VUWLe>6Aqr3c_FC*vKrTJFUAiPo5+Wu*|p+VSNE1}>fG$qd4iD0-(&_Ne~<ML{kE=4>&&e-x4hHTWW{fl;TpisT*GW|yU%ejI&|-yU)<c_5SWp48)#kCDmd<9G{w}?A2VtLy{zfTpD7&K(&DT(A|v!|gr8f1q8^TOq>STTndwkn0&&s3^NVpsy+^G%P~+NG_d}L?k9Ap3(GRYZ893=<J;X(PX|^C-xm^6oH<;YGOjj^D5B6-3u`Hvb;E=s{doh5h?I`4w^(k9dOVR+!MQ7?TfVY_-fZckMr{YY>YH4U~7G(i24}z%~z!c`dHQ^gGEGjpk-f-{yVlY9uh*HhHb%CKK*ja<w_`Sk9@H>O~m?4<Nm!{2kZ>%AYRTiwau#^Q^vfIIAcVx5mD-TKrrG$Iu7lVnaMVUFPso=I+b|p^k?`hc*IiAemXnm}Q=QehOw+4dgN?MW;ZYn4nn2pN+I5{!*8wK<-e((HZFj2#i<0LAxY^zGKn!9s_g_ULj^D#p(sW*-}IvleUoC#)42{$mKI%b_APIMa8|9sS{6x1D_Y5kC104FF1wKpwMFR67kx?NgcI>}I4A6PzcPG*o>%EeH0l9p#-&*nip7^bI#R&v}A+Dym3>o6CD5-JqjJHOaBLDi=w&`I~IwAH;(OM{R$u2X~yn70}D=50N_n6kJKO)#C0E0{!CftlPvL*Au%yj$Qw3%0TN7pA`?y`Z?q8Nn=Ih@V>=k`>I45UYbQKW0KOUqY;hdJuE8gCmYxaKx!7m={?FLm6kT*t-N_PUYRjAub5an8AHoSN9<&k~@^e64$qxp~UsD#-lQF``!9-0Q>y_-k=o1lDTfKuRHb#zx%U>Z&q^~=dXEDj?yst{cHrN4{@t%>5lBWx)Z9BzjXYTwv-Q^k2xVcAM0T}f+=OwIcZGlO!3i>9-k4d@zrru?ZgWI+4;TBDTF3nC<QR;Qdgq}GpVC=0rNHs1T*b|*ha7$Of0u^mImYh0;ME;hsL$J4B{8v%kg7`U-Y{2-d26l_!UJswcvM~F@D|po!z)+j^>yPH~H!XL6=!2D`D1<!zXQ;YeJkoN?bC(yNn=c?5w6!NrTyvtUXl;y_29A&GoYxgguY-BqkSAww_U{Ey*g8>vnf^k&bVgEHiN!6q*OSSgl1|Qcm1B5V%k3{2pdFmnNhO@ojbI*`Pkq;*v22{4t~X!jCncv^%4-wL3%Ev^$e1?M|k@MkW8*?!}G@XVQYx-PwJ$Xaw_>Nuo4#GJ`bqvC2VWuF++f2y$4ptc&bY*13eE64^3dNKo`~Z2V#<QF*BT@`LQ8M53&r94rb1&)msO5X#c~@XO)MZ49_3l!G~QcE-JrqzUn!L|9#p^P>twEcG^4r@i=`C2@tnb$-4fYe8dcOg^xlKqSREPwDEj#w~uc8My3YJ>X1%g4JSDZGLGnp&)LXsGv5rb@JJTgN&dA_O<hioi;--qhtVP^bUbJ1gxC}<}i~1<{zkEQyAoY7|bYs1vAU`-a^5h2ZI-i1=M(bZ~S5`3&K)D09eZ4B3-d0_a*3^%n-}Vn!n$r)oHw`M<0^p4a>MgIf^4d8fhMUDd+-yK5Lw`G1UZ@s3}NM{e5@==>jMZiEz?MOj}juzT5-UXh;amW`O5y{mvp+ztlL&@DR^>_b;&#wb`Q`&_sU~&uh0Avt|$iuzG-=@UE-DX#jmE!HpJxw;2QIcYa+6V6kWbyKR*;r!wjfGk_%!9ym-3JrSxS+}phvK~NNumi?*(v#qKZ?A)8!qt%WunLR`>UCGPA1;y-*uC}!|x-RSh)=ZxN`A0Ie&3~0gGA{__<O4vtt`>!Id*=#mMi~3)V}^cuSr26dx?}<%%AU)5(E6c*wnVgKbeg2Rlo1fN*LE-VQ&0$!5CQ#PJ!oCpbI;NT?Tev<^)VwItYO`15D-C$wx3Ge{8Yv*F(X(ndEAe{b9n0$N<^;hUW_0p1WBw(mTLT}t5=WtilFZz$6_JqcMaqPLEozf^!@pyri3Q3O|`AWHZ_*q88e9gCB!!O&MyWNaTL|;F(nPAcNw^2?Bq4Y0<)W;Qsl>acuS$0@|j?^$YcSN$De%$laeb2ul%B|gAlwwYo#I;crVpyIr#F1AzqJaKCy;D`i6`l?fT_ZZn8yh0XodV0JIUI96Jwc>Xz9A)TgDkN2K_}h@3x`3Fb<!gR_A5&M)>(RP#yK1MmuFVm+2pK21Lu%*$*XwO3D&JJs9=1G!PCotejJt=VQ5&57*xmNK>X;b)aF4O^#>;j@a~#%khhWB4g1E+N!EHopLUR3*x-AgyV~YA~^L)4PlP)?uB=$qZy&9_wM;vwj0&mg&>R%IVUmR?axr-)4Ur_oR6eiBZ&a@BCs$M~A^I1i%bR82!N`eFrmr1M@U)U%!FmZ7`$Xz$_ZfnbpQ%=bYR7@cS9q=1*p{3%}r@JD^HZ_0FQ&IHk>YIgN9<1VIJke49PQxm+_Y-;eA(gj*oadFaxAaL#EbMQKlE25Bd)MArb^`?FR+RX%^GmPY*^QBYZ`q41EqUbzBY_Itj=bgO*8LEqU75FI0Ed{7;zp^Qs*#_H0RvAVpoE6_-n0HE^=DWhiG`=h3ZhFrg^#>7GV24<=NN=Tl{n4w(ZY-D?+;mnN(!I1l%%mm@=)>HSNOC{O|aasHST#ci42Tk$#_&vu(H4`G_s$=69U#g&(<k{7jQ(@Uw0hVQFZCtS|m%<Vd1evVg8yRvjU9;L&*KG7b`=y$z`~LIntoP0@1~VWqYd)_voa?HRL+g03w5E9DoW0HXob40(E}p;~GgKJN#$940J2?{XxXQ69ux>f)yf}z!q?dD<gA$W()B)N_+v@J5+%L#rMZS|69JrJ9dqnhwwZ`qNHBNXd7rL5zpSS>dj*W8q6xb##$Hp&)5)_x3?xL3DuB${|K}oLhNMTO%*$km<R$40mP`f9KF^%7CvslZj-BaCn*NRQ$ILl38E=75$&wcIuVq6g$c1x`E(OXxecG1R_kPYhHj~NPeKh~3uVId<q?Y6io85Ul9iy8`Lmm&Sf$0P7I*6S%zihcyZ{oU{PID?9kkHu>u`?l(L|CKv@#TBK=4E$F<vF~^P4apX)3`;807P9P4+nR!oO}z8Sw?L)fYv&iMOE^Sf-;`}NhyC@qv4L6oLz@ZaZN^}h{gn=b8NCf=lnu;8tiTNMK;Y;Wy}ZZ}%=GdgDH7BpATL0WK$sAi94sy1$?Od<xpR$u*CCC`1~aA&%$U=LWrdsnQq}0$?Zx<^B2n^@g2vBB(I+c)G2dnUVaD$=3%Wrc>q)>nMlo;D7&k58IM|dH0BK>afmo*HRlJ~Z=-T<kV4@OH(+USCYw)U8KL)V<su;V0In2-*FY75BivA0jeg`wK!ZIBX)|D6qY66dqUkoLx5M>H_aJ#MUgWK${jv?IN@R(tL!^g^}h)3+mHpmv)wtQp5(w9<>g22m*ITQrGc78FKs6vzzCje$r^DGUIaM2hWV1`SE5Q7<dB|aYfGG`OZi!=R`aTZ^U7x11nVkDP#>y&Yh;>&*(9`cCFL-p4=>fE!fMh$AG8#2P!$qd5S$4aNemW_6@ZkyZ5dYnTlCn@Euz&>Y|9*R0&JHHsr1i>WTpW9Yr)?f}H*kXZsn=zQ_dK>+olf?1-g~1$zm=l;~9fat2o~OfTKFv)H4)A1rc78FKs47${YQQsT6f_l>O-eG^U=FhdCi}xsL<fwM4JM@xO#X7<#$b$Z)BLIEp=Vy7wO+M-tf6siZ!^KA7xWfIB$ehk=t^#1)z}i-RPwV~&{cX_<$$fX3qhV&Wc{eTuFC7P?6xtOKW8gJ5VF_KFV?0+s%b8;o?U9IX)hgw&ZU{wbmRl&V}`}saH--kpP(f3{`9n-xYzi^0%bHR2eT$iV^Rh9-JiAk6GCy)65Eh+(^~sIoP^-4-%8PN7Qc%;G(`MGUxQBF$qY{2+xordvwf*U`xN3Snc@m4Dc`IIsz3PVO63Pj@n9E=5GA@;fA#An)rEXsP>N5WU=-@@5VY4eF^{5Q4t)?J!vlCS3-sD*mF1xg!<>qkFsCh>krMW>R|Kap>%bu_5?#B!_}CQ$U<7=PZFSdLZy2O&>eKMkv=4<Y01j(8`RN!c!~lw0E4CtgL?m=%gmZb$N5R11v+;|e^a#ott2qs2p<T+Op^^W_rFBvpH|Q{vaf1%)sdy_xLj*$^w{S>e`4Z?ha>Zju5zii)P-FzQv0g$0yBb86{=fB1_&wqQBF=hBrmuq2w>R1v&n=VLL+34f7Hgcoj%~yXuZX9xqZ}66lfT5(`E2}R=LPknz<FFfE0lFr^>fCq(&5{n1G-2jGq^|}>#0~Tb|xdc>yFpG^%xHEh~(PN3H^2K{9-5*((i7xxeCg*>NS+3l^UPJj~SlBll8k-I*Wy&+wyv)y76K<gIN#4V<~XSY6Ql<*Um2nGb1p)^Ey7Nt2-x3!^2-`so|Hm8T+O0Jw<f5R5>IwkKqv4JceW3Z&K!xPA@NRt{zP!vvVS&x3LOIH5mxb=iu875MQ7*kWeMb*SkwQZCgzm=T7Zs^k6^cY&dPcp69-od?m@3M25FSCS_HC2uqU*?j4(JNmCcja+khbYapXqlJ|g}p{_>cw59o-Q5Kxc*l7oE;Hjs`TcUQ|=2kF0tLV4nb5b+kw+!E6z%~{!pm){5a}KUiYW@3W#MzWCL^7g@fQo3Mt+~O4dzKK+6o1TUrubtG2dP*o#nccjX;uAoyO{IR<7)_mEFDJ?4XRxm|J+wi6p=C-r=&cUIPa&f3-?P}vQQl6Y@j$E+781KA&AB7SWT<)`cA+R{6h2hmt|nVHdc-r=fBeL@kPYgEr-@5=b1FV#J!in^3uy}c<E(52o8kNFN5*rRen7PDS&4$#YKpPSS};AfotRUdMV_+cKSYTLV;UC)7VkEZWhDdhS>~cxra6WftQ*@_0_bVwIOXoOciJMuW-|`@q1B(+}Gl@`bUvnwr~K!+<*`Z#bNdaitUyBFfrO$-VGGvCa$jdRO;7%rd^#I8eBw^z~LoyDkh4E>!|(yYSLBb0E+TAA!BxFHe+8L)`MPi$Tn?Nwb6QKW<^_7v)MCk)gJ(vUqV}DB8f<ja@J!kb<qPPQ|GKo7LvCaBbhGFs;>iuVdTFM$h6Le4qnw6#W-gc#?`otAvp)#QEy|!KD7SzdwesThR}+4brIm(m3s}%-`NcD9o7?7N+>Rwo?2W8#vpJg+G%r^aL8BwBoZobcUn{^lFXn^G$z<*P}9|Df<=2~v#u-*>o(_)K#d%{1!}detbvh}-;j)rFaq{6tUbF0%>Lfny%#=0wV{x;{znZc9clceHzdF=qmLPN89mk$ziy;lO#CRXM1d8osPK!~ANY&hoV5@m5>~X$9~`{W6bOEgGAIs}CgHiGTo-pj4VPOzmb?|}PG%754r@4SjmD;AYkDd9%}XVu9p#YA;d<q{K$Ht~Sg?&%v}cZ<dJ~tF9A5x3s2vrbSv}1ZU%Y6LyRrvX8lBAqK`vLe^Bv^+jams!T!Yx<evM9c;`(|T9myHQ&thxSnKs|AdK;^!utwpYawFUiB>n|>gIZEe#j}d1>SD<X?>1hzk`^s^KW2#cWj$p+^?NeoDU$W-4HS1uvf#!OyGC{<i7XlDsy!dI&`hbidP-hW{RjCCaDt)|wE+^<^3Yec12@g}U~s&C%!uQ4wti25@)-KSsqE5ty#~lREV<2|;JQ4;$}qu|G(DfEC;=t7>R<BWxGNyE6Ghf&C))MD+nbtnYDlQ!G@HSld|Qv{kYvf`i%nknb837qdj*OYrWN>*F0>S>96F<~onH)QL~n1QXiit6s8sY^XILI1j1Gf&n+bxM8iUUFxr+t{nV!olZQKGY^G+)mD9s&a_$h9^dIKsCgKhpmv7%4k{~qy#5RXGdW=A~j{rm$obJAuugT(i-9$0+C7y7GebF2Fb?P7b4`0T`GwotCrtRLZg?fhc*1%)Y+dcvv5xviF6Wu_5ow`x8Hb2bC%;l~=jgISY|XuZ2u+p4JNRk-Xq;Yf;QZe*JHGCu%byS*4cP=l%&W7N{oZFTSTrcSU+ohb#W1Wjh367*OPp>S){w9a1<DvR2%G!gn0z!s#I69JN+?@~sG?!EJi8z3A4Gm>r>t*e?H-(8%K4BJINX4EcvS<{hNP&oFNa0q5SHOaV48x!JIx2T7sms9NP=h(MI5F_0?zZh54pVXQMHLh)SKV+#lWfx0ncrpVgeXNHVi7)1MsPA6C-~~)BW2$`CchBPh1=-8eMhIwm@7-PuAZksjDRkDSY+Wr$11J~Mqr(8+W`Y2A>q)kPGbO9<q1pyh-UDDB+&?pbDTo}NT6uogamfRLihJi5g9&O!lxiNXiw!CHtif!2Mq!;Fp22*~5KQ8W(EhtOikN)a)#ewz>>7Hy9ZYsdqEEjz<tI>9xOaXrn5c4;nX?)$ZmVV2?&AK;mMxXU$qbIx$9i~$V^^}3jPRx8^8u4w+<KC@v7etBe2Xlst>6{s-ucB~qOK&z3COtDRV8`J-MPX-ezSo2m?4<dyUQFMj#&!M1e1K;z>Mmcb@&;Mml>Jy@7Mu!U!UH7Hc4UQ32H;_txMDkYh8_Qs}zuQGNZIUynOJS%pkdxiwVgc-eqbM=-E7JdDZ)=NAE5RU5^?%HfPV;1v#Qh!M*c~ofDLO>erKW4@+C!tF|;aIpaEaxPW<^fpgy0(@QFg3(*AAiN$8mLX;J*GhIVGj*4d}w}-Yd{MWIUa{Xwz>I)#x2yy|80CiPDR**kxZ4N^Im<d6CskI%7L(I{RlsImY5~reY=1Bj%^xnPmi~YACFk=RnYh6{6?sCVqSQzs*GZe-g)_BxWY`<Gy4rAF5;|)q7tgClqL7zSgos<6KR{UuM=sA=Zl;H&wi&|=+i9p-xP6SH6rIl`LUHRbom=nVDu^t90m{K;Kvwf9our!#GXGCkT#YgJ%Ir4@2YxmCYbxt8P8$&68;h4G_HJC{qr3;w1Ss<8cSIsmc-(X@HWQ%*9f>Kiaiz49nb}z<{5q{C@GJIS0MdMc#iPeJNZN~U@>q&LErjh1g3^&gC1woftWiDZfV-0<u`J~k>E1#WT3?>>>3x$#fv##bnZ3(@_pcw7=vl)avkM$(N6$e`t)c1)g%BpPXc6W4<jt`tXlcQ&k1l7PUR(6`i$2LrqLgW9+FMt_NvI*&`d|TbgIjE1oxMXP&KW3;!{8-~bz_dgcOfBM&!zw_W#B0@IOcd#tc?*J4!%1H5sBnrcINibBSBpk4Us)weNhdQ%Ngu16)TB9@0&8V(T22T`+C|k$I4U;r9055fQaUz%F_frORDbzFc2Xiy)=&<r1%hYpWF`n@>Am{paORf5QDq2&uPX1=d-kRh*Oaagsb+Kd>4gOfAA)VHZv1HgJo5|2pajjiwH9~QBa5U6?<rla*tqp?HUp1+tOuAWP_Wufs!f)5lN$KCZRmp9VAsiI<J_*`P(|?G`Nck)A(&Hrb6)ihfyrdWNt2ljFqw{TFBC=z2!pvu#I%Jh+uKWHw?0i0^Be;?)RBE}{9-H%!csy2SW1YZuq5{-=AFzC%gdU-U!@c*xu$O8ykQx4C`WDNuZVm<pEV9zDWAW62rr<-@t`~;!bvAFZB>=~at~0WAzUz<0id__JM~=sR@*>{1!#1<#75NSlXgTC{Z&A(-CoSIK?uO=0(vIAt_G(8^qo;RS^(Z=44~i1dLe+t;tuS#w0dM2^@kb263EaAo(F^zsw3Rny%<4I6cW!tx5l?s^_-o1cYCyY6DG5V2&OA}Ik=#hjndUN-)yvcqwH8l^8C*~n4xX{%RHEQL5L?G0OEDED8$>FS7<ZE*k>Oz^x4aLC@Ih-M*tD`oYzCw4<)Z9yd{fcZWV1u;9lFk*k?gesKz^hFR&iEF70_}>4W*jP~`fUks{ZyZuJO=phVkeC9cB_HDB?2AXG1W+@~Njf`Z;T@9kcUASepeFJ&}9t_ao;!-+=FcOh7@5cIoF?1G^0l?3|!lv49UlNP7iR$81I&Fl;t#QzdnoO|aNgNYc9YATtM2GhF?+%b0Qn__|4%~0*|V?Dg95S46#d2up)z~u2~pTTVE5r$0DB0Y)_ygzGPk(_IY?8i?@FDL+cQ~-)KB+@rz3~ASRrgAeidJE8D4hEo&IOW)dK;$!<fcm)9`G^$z7?JZw_OUp-Cma&IcYd*Vq5@E2(y!KaRnM!GPXiDJ^D-Mp?bRRTPWAT~vR$+lE5LOnyJ(JWw|AAP?GHb(#OaA8`2?R>^fngfF2~6AyUj&>E^ipx_o!r)TX9;GmKC3Na(j1C=Q^x&Ihlc8%ws*wf!1$8%#wWyE03T{qjWjrT)%ODngivjb2k%;n(m!n%m?W(n1ukCK`BH(c%<)Orf*=LX7cMdkh~3M@Ee##gX!qNhD6KMzC%$H+U5_2w}byT$|Tx2qpDK%&Z1g6>uM&aaW0o6sbHLMvxhjBYxw2+k)2~Soi<0mI&Drnb&AVuQGU*K!2~_^{;Y+l#7HGkmKu-I8vq4GrWy&eXlZws{hsbH-6|n)0C+Y7K*wkrAJhtJB;%5uu)4G*tS;}Y3pBDNfam;J3Qykqqko{|6zg^`z!Q{^>aXoIoj$KB^&e_&3F0il;mJ%8&u%?+0@h<O+6Qr2d#+uLqjm>P@%RwF=01=h^4|Hyw<@S6d3HXlu7<t}z$`n>;|gZE#F~JV$YlNA)lj2JGeFK|b<PIFeyiqs8vgU-uJ_I_1~VWq>+h*nk7m*mYaI`k)*)}4w6__bw0(Bp#S@rgxC(<A$QppD*DCRjs~kH7U715)kDWBq%c;;ojmbAs0qvx1b$3$kS7fk4-^mP4+{yYqB>KW<<90q9H`wK7ENk+8VhrR(13pH;5P5C<Vkkj%scAZD5%9W7<Q0_U8ru}6M4!zN%4Vgd^bgg3vKVvv-4+UT(Pj(0%h9HCobskP*P<9o(_TBj7*|A}?F$Ee^w!m=U9_<%WP_{sV}^?0kM$&pSjfmuyDhAm;Dy&tqlQA+B~JhGSqZ$2G1F91qc`e5i+e{h14>Fh7O#2m+p6FFSMK-~my{+m@L%}^zu)~gBwPD3q)mc2WZ9iI_<3NMk0qtT3CX};JHJ?*!XeK4Sx9bL<-OF;edd?w3z)YVgIV@xIt*s?Hkgq&FcYx?GsFYUV-2L{$$ls(eeL{WFbN76kQbm}AWR5M4wk;~WcCJ_+*!!JE1AY*gBjBXX3S~BGD_yZR5p5cdog~fOq4pIq{7cf(I+c)UEpOFV#e<>3%Wrc>q!wjMlo;DIP`!f{07g`0wB#3OZZJS^P)U~!l7&D7lVmvL@5Y5S%X)_`Z0~|SE<<z%wdMkcv(;3Q1oBG^gEb=6_zO;g=ptn5@esx#xI5vRfsYLJ;mKt_bG1nSIZD?aCpqH!Qo@&Q^X^7WE*6QY=bYmK&daKoUmLxHxTm6Yv&h(i7G@naROi_HQmzi2p8G00cN;V3^ACY*W%;BFLO4r%$e7Wv-n~>f%mKtBe@)~5yq~1;}?U9%0u<nIqKZAtws%M=OHq}*vSmS*vCqz!<LPHvTmbb)fA$TN>55T%DFE|ra%SoYv&i^njo&Edv)7t%o^9BC|fLCZ!^X<UGJpd(~~&f!Z5BQ0i3`r>wrYR{+6m9_Sd-+3Giopc78FKs6bRIYQRY;rG&t2qLj%7bC@kK*&l`_I$)e^FkRZf<S%D$48~YD&z}kcJ@fjkHOl1c5iow#V<Wzx(I_IFHOE0$b^EFYm)ItppUr}<)ypaeaJ^j=^79~5`@hFC{3_v2?&PO4SF-brBBX2Q7t35C$u$>P4>h&bw3iV=XWUF{YVv{dF~dS`xFq$MPf(J1mwJ*F$}vl^KpBlHLR$0!1jR-7XRRJUKmA=^(9N3&!CAkRqTwulmx5@B_>0#D9lMhm9J{ymdjn|uQiqHx#8a}{e3O)Ka+#x5tRzrol0Qb!f?X_mrusW2<@y<4FiM(0?I;k1pq;iUeiV&z=tB`1p1+e>pwm{XEN^ZY<y6E(IVG_oU@2jbfJJa(v(6;K($Tfsiw|HySVlnU*j9G|_XbD0215;xP5a#Fg5|K5lgA2~m#J`B>$oC&a3pjjjdNu#>4HJTXX6(`=@FEL$Z04G?b06&jr=z*t-;#3ONW_^yL4DjC0!YsBN)m^$#lzDzJ$Nh9`#QxM?9-AMHC5xZLCpKX|?50ot~s|-o60+0MTbXNYhurq1=0MjR%>@?4cu<J-IawWyd!9TeWF0&@V8DkWMe+Cu+}U;}<(Js4xZ2;~H9_tgEW!Gj@d!-)14u{W_V!{rXr><$U2UM{?XwWF~bahj>JC(F8BhVaLudhB6^Z@W!&MplqvNLpj>5@tORX;h8*Hzk8*#NE^B>t=Fp?Z>lqxF8E`Ga5=siHmY1ZzZlGn!1T`RSgEe=6e<l5e<iDiU*2Zym%eu$(cyySkj(6cM~MCblVjYkQeJWwGUia{ioQpl3AV9DQfs)?q~tP7d-Zui*HJ?CC139@?X+z*X`DL~pwXrMn6u%u`Fg<nUI~^YU-Fx+EFmeYvP4L7>@Jg~=N_KG>E$v78C95S=%xuA>S{zzTbkb)#lp#qop#U+p1O{_C5YE65M`?|u?l}1cgiWjnNMJXPc&c~i$^epdd?)F(nPQReJ<-w*+V2jng}S6CfeE`T)4*#;n?xVjK+>X)^L!Vl~PRo(Xxtv54E0IeY9pjM@^C0o3n>1v);WTgEdh^s%kuuJ$)AQe(Jh#Kd>bW#bM3{isSw5FisJISWJ-BB&Kli&eswA{HAJ*5zh{ri6NkR8!KTAd6p~)zXuzUYPZQdO^}{RgH7C9A1s%>%!bQe)`M6<80|F}Y$_|jLJHy8Wd+AkGr|$k(p?+B*JUARw$qm@XWEO0jZLKsa4~F!n9V@zdsyQixU5NXABJB}D5h=Zss0MMe}$ipjo*tR<joeZl|vd~J3^7U0U;KO!|V+d+e`Xk!ZeI-Pl#e%&ss&lQosFlreB$H#<%B7*jG#t5j9f#_0@!}&H)hRZ$ieT(`?4BIIIWV=a6ldt5(=soiQtxtD4!KS+4#7$ovwPD-%h?iIlS*dZ~*ZAelPFRkD!0%^1maQCxi;KMW%m#_m|wt1<02W1O=L<7RX#Uu!|`sJAiZnwDqs)@J#0fHNGP(8_sr5dhpJfDQ5B*$e?5))Tc#C@z_<THGxAyRdJBqP;ff`5*UxH4-Xs_gYjnlFXoJM6^A>bRM!+DZqk1vssrJhIOL_K%hnr-U7AS+;aX#PJTl&Hi8K(13g!FLjt41_jd1vk5Hi~WUU=i14>64Kj}>iuo3BFMvX|1^~B8^DHjtz%4;wiFQ&l-znJ}r?`WPyBnLEp&@O*)@MbA5z!=nt3X4<eN?qL9H(YL|S@Kq(JDEYCJFMZTSQ?uiuIZ!Wi<x{>LfSD7xg2g+p6SqYg6;~ovHC;FwRdw)A;%_W_5zea9jVkmp?an%zIf3ncU2H9QaYOnqFk<I=R3;vD>d_(uGftVNzuv9d0!8sBb9@!dc&VSt@(P@+gM$P(RnG?&%*HqfP-36>!?)mR9!4t0p6w&SCXRz@W%`RzpSU!hs3;kt8Kk{1I3-fEV%LBuAxnN%WZdH?@wB2rj#YtkR-aMER|k>Ca5XZTPf?_{l2OlxM{ZsgA(>*MwGC#^?L%8$6N@+GZTqb3C>~RKH3x9MK2L1xH41hB>$P$ofBO3FL`m;6%g8qB5Sl0?K0x+eN8%(BvgQ!&EQhLt;cLgMte4Xy-CI5jIEKr?8PWvC|cnAx~#P88F}@!^NVqf=t(XV%?8_!p1n!fb#CS{`{*#Px0xWWsZsBIAJS-Gkm<ziius(wyfYF8s^QNm$z5+@r16Ee`2(>F-=>;>k9b0e$00?tBcAr={{fmgmNT0{3jA0P{J-G~Z&r29Y<jZLuHF|o9{JE{cI4~`L4NK0VowIuDw298s(8AsmR+`{v2wQ(KgM-711;mn8ouM|MWXdG>zkm}45n8>wC9AwD-Lt36N7a4+U&*ffw~m=i_2z(udeQG;nYcbsgtcB)uG7@REHkxAv|t<oYqM!mjdqtUz7IPYRWal<?_!<EZ#f6xI@ArFeB-H(xywG81~&o8_BSr^kYW-q?a`vxe0}1cN7RF*XY=&9XB$?t(sBK#(ByS2gETf%3vTMzIT2xuBd^jbr)(}+v<MCQt#R>mU8iA22T1|5781|?C(%7{-z#&^x{kQyIy=Aq{SemnGad?*mLjpVgOO^QfrRWr)*s<NdqVsS){`N-e!UTcI!#{f-@zn7oyr^dAH*{=!0eeQxG}4zj6q3!vq8I(|hL^g9++MlxlLWD-iX-$r{W?i4@kc;~C7y48bJ6Nb$dWV~u#M5@NNnRYGLRZpW3~kv7z?T5bUdFy1@A7*|wT%FJ1fC%4tIOM!8Jh0B%z<75V>>|;GV($%~(TNz5V`Gwh+$;GW@i5r>vDHK|oMG2m`*Um2n6E!V4PCy~Pt|~Q6?tT^)DxC$)#|*)w-hAfhaN<&MCYUus+rW(K#C7<Uj#oa=3|?^VkG^f{!p0L6jM`(Es0Y}(8r@btAnDXdX?>Xa;5nH=nkg5rk~_THa6MPqJal;;Gu{qe+101(sYAz}?wKhxdsIHScYd*-g0c{|nOXHZ(^mHaFAcKKxQ;6>xZY;qr?>U=>RO#3L=#tM(&V!cWrgd^{eZg-XT6|QGQt&XW4y9^AxRt>D^+P-L@$6mBgh4i3e;5zSwa5jz&Qx{V<rUor2}`U7%@jXeySPlqFGXZF^j^PBb~ZR(@do`#v2vRsl2;>#07yFGq`8#suGTuJKDvPo41*v<mRx(qe5f*-THDaud;4qBz}fc2rKsefr9b#S;ILgfBu$wnZ01{0+f)r6~1)0cU|2XM#;Bi(`{`oA3Ps(LU=yb!{`N5%BFKxuUegxa@w7<#$Bh0IGS_WT##7Qz4LpWQwUAbPzqp-rmjW}W>QD#0_JTN2xi*#IE^4Um{@LABN}Se3zU+uc6?bMS6~to-OKS~gkSW!INw%%(fAd`h_&E%n=yXfdeSql8KpTwQ*G-c6uZnSi3!WgCys~~0$5mHxORRqm}sOe6iOP*x|;V4CiHHEVs!4$W)Su~)|0wd97I)6I4CABt7fL#-O)ulzIyU3F_Y1)iFUEt3rnnVg{Mv<7vC_q2uI$oN2+b+O=zd|pgt_)k|lEdn4!q=V~q!8)Dm4VMUKNNL0%71@mkLKtYKVYff~k1Uhb!G+$}iW>fKk1MlfGVCCW%AGss9EtDI!2Ihw+4g=dDHwjgO2YirGXhXBaWr!tfrzczj`l&EM_fB8XjQX)~-P!56yf=BLTCJ1HeP5tF?<TmbJTX8(_=8SuziE9$tr%ZF1d&9zeU>hT8iR`MM#}FvhO!y6FE}DpA8VsV_a3d)qeM(o;Hf|D}&A?+H>jCB($_A?irP^p|L7^~uerHkJR=YX5?3|G1j0%GH&M)@a48fcl;^tNF5SUC>m^7Km0F&wX3PWMg8euS_^DCHHwpSQx_r}3znHTbg0>K<RzZlGdz?2XGm=dBWFv)$1c_%Xj^RnjecWL!YZ^})FU2Zz;m3j)JHln>T%wvl&Kma}QH_1JpBxcXl*sZyG!0!niKzXP&(CH+mt*UZg?h$G<WDsUE0Q9zgXRNE=YIQ&>IBFN+5*$&RW!e!<^j87Bc6%`+2O$8f3+VCjx*D7Y(05YaXaRVeF@S#O`-K1&i*m5r!s?M_)E{O5OCa2E>*mSvfa(bMb}vQ{6otfd(5>@rRlRKI-u)h}HigOTA%f}3UJfoOW+OF4HXEs16a0o{B+vi+gBjZ9zs!S~7le570U%yii$c7;d4)E8jD7YoL!Z5@hnfOi+6EADKeE*i%zj9OE%7ZGLG2nUq1@%(?!`U}szNp10W^d4&~<4~L`xsxFNW0D$Bd-DhIK1QKm;Y)J}V)wr>=|}V@9xE__$ADp=BoY&UtV5Vgx}|NMikOQ_W#@m7Qn=eOH(j3qijN&@Kr2UP_?vPboD)G#PXnDQj2}R9qay{}KkBd*>H}iO7&@E}4=B)4L4ZF?N=mVu9JsP)6}%J-n+Bm284}QEhy{<nd>p!KCD{rm4_Mp92uQKWki(DS!Sh?cDx7qNoHEYfPlC$QaVD9!=$Dc=Q&c!yF7m8^_AA4T0*>$0nlQFO@zb#Z*S?GZ+qq^4wDbrwH$zU+kW!1eBQcvvpn7V=U#<0ffQ4%*I)JH3+#=1%BiqM%k9KF~;IbcF`R1ZZ9iS-yeQt$<rfC;=rhwtGBVZdV_c=*H4xg4ETFgGs>+ztqIO*fWDLByNhzyVFk^}3^ZpR>tRr|egk3_?du)7LY{^n&N$cKW`CLv_4DLXL1ELq^NXn?9R{-y05d4<=?9PW9nAC%%+vgS{RWaZ!3^RzFpCCrPCyMB(D_`(uW4YLKbY<gCB1-SC7`lW_0FPtI_qjCr*STq!l_`KZ?lIumun{G`;nbvG@Uj_UY$0lon@tI1R3<8v~;H+aPQArh}N^$hnPweSmhU-!U0s7YADR2MdMxed%nYTtBb(7;MoiS9TRJOP&ufPj7xUL>f)BMy1dge&`6g6o?(%eK|$2@(LWG?s(Jnk-~=TkU$Y3&TH|yNYp!tSMxbEG7f)t_aCYmd6VRm+?Q^)SJ=ZR~xcg^{=a=yL7<NN_P`T>Z`Nfwis3v)KKC8Bdz6!uBJ0au>X1O$;fC9;6{odA)i|Ly+qh5Wp@e%EpYEG2@c^=q%=NE$+5STTg*ILwdRVlM|JXl(Xym8RpW_-~0iGCMPV2(K}45lyE08G7AiFaJ(%!@+P`I|Yx`!v$aNz*}%$v3J2?WApWcT(;*WUwON$qY{1$@)Da`oeePzVO{R;jMJ&Y8rmx8C)h}viD#t@!I&ssDe6ElZDjc;&qkCD^$rfVk%6eKARz`%}UEF0MrZ0Vhr+kn-oh$GY8(qZBseUe$&kNT3kXxx7W@u#uagH*E~MxQ@E~1?X8VwAsYn0A2U=Mf2=1_#X?4&+HG^5+7@{2I%+7CT_yD&ADh727<FQaNqlUo5UD+Oz2GS|fZ|g96MD^_-&XzZzjBANxVSW#f&a=U4*u@HA=&DgAsVN0vt`-+H~4vsn2&X);sTN*zjl7Hx`jiW`RxT?ljb|#>;K#*fr-B0dYdt>Wq-WGxJLg4SGOu@hD5Bu4Dlqs=w|K)F35kconH(lK>-Bv0u&I034zJM5-Xm}-T;$3W7&6E)R=5AW7@!sIc-=*$^4gUNzZOC#t+qwl8+QLem;smWT}e`FC!8&ewSI$W%^i8QsXg-d6~vg*2^>wPNoGw^8Rp{)<4elPo#(<qig3EgNZ6fDF`}QgI5*&F{ABQ-P#SzVTR6lSx=Es^k2Y~9n8QA%XB<gS5B1#{W84#Tt#)GOhM0fx7B^NoBh>Dgxek-Gi-bKSoswEh#lkx5pM=RzU;!M%|B1D&cld>3v$tG=NIFON=P|z0=OnM|I(ld7hAG{Yq$gvF|MK4>f^yhb2eek>t&gXFUAXbPbD#s%V#`{V)e!^#uSy0>Th$@329r68q-c$WCXO683eSCl}^Vl8~tYeOfA9k)hMJAl~VGNmu${FN#J!lc78Fg3F1n+m$$9PtZ^MewZ+2qHe+1V^^E#GQHkTJ4C9(<yH3kGK+!KkPp6T7zo<e*c%PP+4^u=`H7XS~;G{H^bQG9P-ZI%>4zmR&`@=9r2aJ=QsN}SP$zRUi7>v1ZKYvOHdgk?6qg0eLRsSA8dn3s2!9|K~j)N}s_Eilou}xS%n+4sfmsJkndb?8OdG%92Ug=k5`1uLV80nvNt4Cw=4(+w`ixo4Gc$*8Xho0JM+6xMylXIpuN%=tem|=A{T;hDpCn(9hOFgak=QV4wKpBmC!Yl~W`Vs+t_h+pZ!ThP?W-c%2_DzKFtY1sfcox5FNi<0OMSO$K-N_8j-P`)TL9~6VLy{E&D%WV_2T@YaS&vkIa89A*HX6VkTd<1(voC?`%_TXv7V$R}>l3IW1)>nN+cwRRqG1kwY$C%Gcrpug+iI2NDGtM&ir6p*v0`8;VUK}D2$zwTsYO^ux^{c<5iAJH2uL5>>MjP~;7Hf#sNu3{pDbOl9M*DjSt0W}l~MDV0F*sE5<1h$&04k@u-QKwzZgo7pe#gALs@8-3~6ZOzj0|D*v4%-%w*iA!+I+3%FrsoP==CBw~YUPb>Gr+NseRpU%YQaBt<=zUf70rhG%UIruqRd3=A;sS<ZjA6pDze$6+WZvwCiHB_<mq)>UQR^HCxx@|^q-09Pr?ZBr8WI!@jrgx77XVN@SNeZ``9)Lh?iC@Vyt*GZdW6&%V(v##+t)0sVV<UFUh#-Z%kM89<bcaTS&Vb)<}@u$c??~PyV$e_lgoZ2-sLRnUqrqFOGgZMfuf$rDN4DQ$4x-0DqKOD(^I+2-{BU!^1$^Lqc5jt$!`NdF1Brjfhco!(^s$ZdO{n+?SzRmDV?yS#V=`31@!=~f)dJ-b{U^?}?8sVU9nH}t3Id^_Bm<fUDonK?6vO1HjR6P8dt{Q%MnXzB`qZx@d7c6Tu(;HR^)&<KU%(p7@d>@0M)xN^ic!!r%XWhn{U#&a>>=Ut*I^O$z#8Iuums@Og-?|!C#1Gm)qoeyaC&PWy`K<R`6_zM=rqjqC`%5(~B?^tQZ9f~do&r3J<I5!t5~?<N56B+MYC!Irs|Ol|!_JJ|w{a5gdXc;(sdp43qM5dOUSi^$_E}HpvmU`2?6Qr;KUnVl@-xtWuixsgB3^%j{_(&4kN;Ib|L_0hf7TEG*MInj8E!Ha6p^lJBp_=VY3hz}dW|T;QRR;rjVgbvy3w(foK1P+oJL7vH;Yae&)y1=?B_7fjsIp&`)d#)bvEuI!Ovov&t9k3lUuYPZ02M@*k0r|0~o4ekw<D9&G<k#vwEn{1Ct=WUV8)~s|VV~s3b=6ob>+v+YX9&w|&fj13%G<F7eU)U^(b%HXQV{ZuAVa_1a)`$FbCcHZAb>4k|wt72g}b)IlMswp%Y$PBfPeE8EJU)y1$CqBjGr@MaA^;GiZ6zUoL=ry8f3r~<P4Jv_8+{89)ZtG4)6R-`^NAqbf(3SvRn%-(>oKBI5OPc@J=eOgQd^Sv(IEaki3GP?Hsz2%#R*W}-rn&sCZM65~mCwJv?bq+u%e-JVzpL#R)!e-qFMQgGNu!@Zb&Lm9>0ai(A_XJo!fMWU^6JY%sjEE{JrF8<PEP4QAJgBdt1><GLFvgSm>TxVlTiy#%j3b>+qiXFT41F45T!&-nmY^f9+ZfS~ei_aLhb311ys`-3Jmi55Vd35k;cV6&zlvrTO|PtlKV!;N^Xi}}iEgry9|?Bys~r>-gCsGi1+|~u9?^KVn_$hK-n>>1nss6ZK!^rbZy{Q2GMux4o9cQ8V>tQiz*G0|`sP5Cb8Yuh><ATqG_B$xC17;4f+t5y0&Fz;m{FtAZQXJ6hT33ffk!Eg;8nim8NAR>{HpP$(mAyV@IkbVb;1strSNTtLB%JtxFs7Yi!=F#OSRfc-U?+qGYDmyRkwnrp(^8={@KpH&Ob%t46&xXxkLxDs!Y&T!8QiOsqA+DAAS2uB~TE`KJoJYl705FB77)<V13fwOc3FGrZUe6m#@>xR*G%p$_Y+pj{AD=?)VyHlt4@YU8L(a7Q!X|maJ8HI=L)ghZt0aa+0)!O=U5s1>!nnI5HXy5Z`79@oC*9GbH9EtCd!2h?OYJbY<$=)A@?4A0i|m(sh5-#InRAA(dM^dHx+#OMekcP#vQ3J+EE($Li{@Rj^$R?$?hQaliJ~=aeS32@jl^R`PTs22MJ}vva=cQg=DRd{uffvlu^b$bG)L{72pzT`7V(Qs5QpNV8gaeHxR4FA^#}^=5GSUe;~8BT16^-jaBFa3PsXNjz|_z~^uoQrPnMNA>!)^IMS&=-Ev3%JN#p%jVu*cW^GZnMH?@yvziVjFm{|dAgz^L8i}6s?o|kvkNLkO}m$~y~;4!#(H<DQ;_96j#93UN@?<Ch$DhHG*Ouu;y8EdH<Zi)n%)dD+1t7y^3?|htYV|lS%qfxJ>{^cGwY{LlOQ_DYv;E*E2uG%l*4QZi0f*eC1)z3cCEl;BzrT^A-=8pjHDL{)*Gppmg3w<`XwOt9C5V7%mX0@pxZuodn<ULoJ3y3Dk5E1=XPy6*mdb3L_vB%of+r_-PTR4Tl*`mqqf-G@cJTEj($Y1gsYslithcLDd?=dc7AJ{f<s6KTAMt}>dK4nPPRveO`ab!YVtg-aZ4#E9FA65h$M%emVO9yD+l6Kg(zoY9~a~3R(h()PNTO2_1yWbprZbv+U2)`T36?@m5x5@Y{?CGX5gN;brT5jN!SiG-t);YduzN`ZpTUEJvE|Xkem$DrQmG6c6%#&sC7tFOzYuVR&!j@latcXVe~FDLG%vmPO5?vMXNcXHcaFJz0~M(di0|9VQR#;qz?S{pr~-|{8l7EL5N~Wu-7U=$)eMWWTh(7aR|6a@-{;xiBAUmXFn`4j#c$5HXW*dEXmDCvfI*m`jJQ_fqKEU^IMTb^`OL@mg(TSnrEplKAy;uC6L&e!HIfXH!o=H8tV;2(&aeh9ZW7vEkj(%&bPA=eb11hXYOm~w<3v}h8#zr-(FT1sY^cGN(UaB0m<78k))&Z%hBd^C3Pl}CH0$-49n^248@S$;Qr^MRym;TaL?{*{5F=L;8UNeWO*4atHDjx0Fr}~D2~r2A1pgF$RXt<G;*6)6&Chvp0ABiw>w{D)`Bi)3~igGwJ-TZR1LUxeydl4x=zWG<JxjkSLY%uHHOVF&IJxgUS{Bxmv#37%HlMbNH%@w4oRZ4AQ|08cGksymRsQY3bwK67rK8We!F(Z31KW?T%T*;krs?^2{s2YzRiR%J|);T6(8ncM?)N@XozE8Fdi7`4ZV=Q6z?L$IOZ3Ze>fu~LjrebSzQ#{<qBM}*5zepsCC(_VXKT-f3P0TT|b|@6OKYyDbMwVbH^TmcYW6I%3`j=`XvQQK`KCB9%TR(Ag)ELwT-&0&J3R9YsToN?vf9fw>cs(Z|i27fhi`_EvZe_bOBKlAnoBS0o8V(Y)5nb+4-ezDTJm{C@L6BDXU>cGA<|RfaGNsh-91<FpbbwO(3V^l^TEffTKiwZ-%iu41yQjo55oQUhr#4y{`Jaf|nP9sR6vpjKMpsJN@92A(|s8Ts0~O_*~*8FVS923m>qtuljKEC`U>B;#z^A;jof6#TCh#OwCz|9z6lsXov63AkMk1J1MOYll3@KY*9viN;f;A3$%TsWa(MLpr$O?#cB`Y98=`VSir|r&6hET(_=zwg}tuM+!%BWpSWnr3EyTYCwyDOMuRj5r>2~+j?(QeNAat0a5Cn``gs)=q!t|I%`OTj$JDJY)yHaH;Y(-6h|<l@4ARZpDmyi34yHg_W(^~J5@%7gtXr9-VFHk#*j?NBtvI3rQ2DV3iAWAaX~nTI8xTBdJ2OEX^U*n<H%DzHz_nGq(<J2`=XMZRWvVwRrAwiIDq#rZx{cKvFFqxST(GZAn_o{*&~O=(S6+t%X`!O0cs57l2EE=4ob|SDKqe)%Iz@`j51k^U;-&!zs^e8VZygB42ujGFJHORk6GXBJj?YL2?+}tqR%Fzf$v~26`-(!^`1LT7VN~uWO)|Zrkh=ZU?m&Bhx}~p;-wI?#K#D2=QZ&c{B)NwWw=+W^PiuO<Ka0C~Q^{GQqzT9{<Jc-7K((bGQpxDjygzFkupyR2mZ*x`Q2Al_HW30S0Euv1^Od@~RP)jijs`<Mpf>|BFY7bUTlq#qtFJ>a%Y8otLsW-^W;7%HESTqRZ>6yygkJgCUuUn&s=5_DfAH7_3%$#X(epD!E<`U|EPaP<5>?Nw$k3ygmGDep9B6`2vEbV7t?+@ekJJ=*NhIs)@*JIxw)0>$7IbD0;frUUvT;5!8=8x4Y-lbEGkPUm=U;#KLfiaZynA^;5J#^7;<B0-i0jjp)&XAZp|=@&=xN>53h1Q&hY)+t<N4~Fp4Je*lF={{(@-lwz@FQ^)k8t4M>GKHy>h;~IOmx8n19cP64u9zl(3q0Dl|a&B-kD*Ve(LEmqQO<x!7@M0r$bIN3!YHc5j6blzJqTWNPW$DywH#_z0grgjmIb&(Auo1AP7{5zzCaNJ;%nj+tT`Ic7@aI8#^<|1KOe*UoQ65^)8UEHTCvN$(PH#BlJ)WDCi|45bD?*3AnEmDJ8evW6l9lHC66J(3h%HYnf+%^U>Z^;s+HD5!fW?#k-(*C%W}s_KLi_pG;K&^W6rQ@&UZy@h5otAS>vCE4~6loTzoiKdT3>5gb&@FH@4TMOu`v<{BzT|2+kFHuz|p0Bwhl9AUDlKg4h!APEF<Ago?dfcv<-nhVxBCJow2#L$d1#?!qzL0dydHAtJyD{jf2|kvr+gSaVbvizV$XT`gvH9E3qdHKkmFAWBt3(eoMZ7!NW*t_P?94!c<+g6dG0R_H%=&wrxG-I8#mFAy@}ul`<CxTs3^59muASdXrRXq{nE;ZyNHX7G#AhVq6Oy|r`103}-bOO`3CX-7*%QndoS1!c9)1!7+x*D%bmnirS}Up*6~D|&2d2~sEVqK3PpwbEAYWz=LC)tq%JWH`nwAO#IW;-#56CIb6eacb%b?1nky08Udwtf5pz`NetQpgn;RLm#68N&LGU_h*IkjQBG&bNAZf^!~wm~u8F#MO;#YH<ab#cl}U78sWsAWa~&HlxcQ4{U;QPVdymG7!jV-SCx9!f!x$Wv}Jjw6<p=#Ew_Q{^TwG<-WVK`amJu6NJH9PINqkKKE&1WvPS#;|?K?gOEc^bm5=w((mpQcy1P?0i~U5{^{>WS;2~M<DYlaRg*HI_q;wLdvF77Tf5QmBwejNK<j#UnfMpc77|8ijXWx`>MoTRu@&W0}Phh#%>(1ml+?g$KbpRJ0#n%6GpN!i5PiJ3dA$0vTf-rTlP{a4uTr!&6MGwh~(=y|LUG~b#_laE=9HC+|CTn+0ObL0zDAOxS2r42~X8PS90tf4<GlzP>P=tS_fp?_^mi1&HHl5u1eZvmB=qRlB;A+m^!>SLmaD>hLS!M#7SZd$se{}1Z5P&DULhqxW>GlvL@||O&-c>pF6)5RKz=76R7pXEvsR5&dOhq4656Y8ER_Z)}44@CL;+wY+;lS%slo7RS5E|2Kw)xX29E6uc1US_!jOSPkCPk8Ptq?$bKb@ud9CcT)Bc(Tr=v-z;opj={|d|Mq46MhpeS8B$?f{Huh|rZ~I|vfwH~l&TrLjaEK<pO3SW1@#mApgk(P6&P*gPGe$Dc4{sRB;B6#>Oh`uJ1xXDX=0rDW&;^D_#y3Yrfgs^Oc>#?7!i11ywFG!Ovp10BgYD~C>N7+e$q*+bLy8lS1#AAPy3oDbTfswRpy&hX3Z4(L9;kGv<_^;eJ$R>C(5-n}ciPz@h<R&<Fp2Jl#%eU6M+1or1Tqd!+!@6`=gx0M5*2}}MmIQ6)h|W(HX7|m71jyKW`=HfT6ghJ@DE7(8A;^@$hbXEN175SxZ5^<D~_lJlt|Z+>bg3QRI@+3esHJ4ZHAo+A1m)d9I@kAl_`#`sl^1O&pB@uc8BM0sIYtP{8l7U4Jbv90Li$_o>VaO<P0{D)Kkn4BdJG;J#0KOB@@V#n*5VlW}l7c>?1qGFb+M^G2wK?=f4ZO?|=$G<p(*eIc8lAE20POj}f$XW)QUAR@$AhZ1jU2w!R<i2yAFcKXTryn)ex`hqB1$&TmCBLL_PJtF5actw=T%)odYonK6>_eCvEpKSF!a!bmn6$_~k_vp#(&ndD|-p8EC#2Z$>^JHHi4RQJgRGT@Wcij@nJRgE#)NH(*DB>T+_!+{n@8%c^2lKgPa1~mq&ssB{)&?nE&T2ItIlt4Jt7Z-K$+x<ZS$&NYJwMu)euD}x8bnU%a(Div*Wdp3YOFf=PSbeLij#A~lRJAcL-zO$22-b7wx5_&r1v00+4jI+eIM)aCV6cp1=JCPtHp9ZKp7L_cI~+-E_4)CjJ*OQ89D`9F=(SENc`4GFKR;`A8fbP>6IhL@>Xv;OOF~$duOzQnW<N_PR5<*}B!dpy&I}IQ%lh1|S-;Vy9SXseL}5ge7OyPlsNZ<yNTUa9>|hs*o+P+Xe(uZNbg1KeyT%?tNl43$psT-Dos7I<tjF9!g6D2$7U-|VD#P0u#yDm%F^*H>B3b7kR|ID(<4hnd1D(6Q^|Td)UI0Xib#)deuK>hzD%0@LI1g<O=rwEJd1#ya!{~_{tFa<;2qd&6WOJ#{JHbrgz42Rd^a#finYk6mOtVTxg(Cfdb8UY%Zp~&U<JN4}U13(5JO{=xO!1F|{1DbwR>N&q5cj?rQ78nqv0gTnU5Og=@xSFX_%hrI;pTOs<yZxW?$ID?Jfd`F58XD;*{X5qI=11?Ji;AyhFAv@lAmJ8yf=QU+ky&_a%$Jjh+|n@S~0^RfZ^+00=hgqGq^l&>#hnfcG4oV)3yh@@$}WOg>me}gr3@Vek+a<X<k>F+6BkD>Q@|F|1&;*Z!<i8JL|JQI*S?Mu%+=Db?pVTN3xuM+e+V%zzB?V&z;|jWI{-K=huL!tj-K26%2o-pN2<XX6%vv=nJCFMamk@l!Y~nDGP@%-=54l?%q>eUmxmtUHhD6-Nq^vmC#?EPwMNzAHGfIA5pEym-`B6)R)z`f_%{G8I9AoIT`Mn&KI}ma;`*q$XR&GSz=nM3_A3R;O4KsTGTbu4EN`o$@~+l7I_aC6Uu5p?wYH|8CAc|jNP?y_U-zDyrm9z*xZPu=Oxz7X`jFJcPzsf7O;&)%I95i?UdA2UaSA0>1{Rz0Fex5B%s0>X=+n&dX4+Rf!~iA4g7wry3tvcoJ|eR97nxeH)|jdy}5?w#?Uns&^Xz-@prw{gb=BWag2+P5~umFb$UIHMGL}aP6mYS?QApKpeh!nVlj<U<ufCO`hjBar!`c;HdZbg`hVh=!9^6*HBnx<#1pOH5+7X&mVcgR!#_{!hS6VJ+Y1JlN4;_tTF~wN6IU7rUblqk{LhVF>YtEd+O2nJmD`(gm&#Rg$WSrtQs~V<%DP#@5BR4^2476$5vy^UHYy;y-@`%M#xI2svP_F#g*CFw;(rYV=8A$?5H_<nAgoW`n~BbYN!@@jOlsr{yGi-(_e`gKYk~`COjqAPZel`+IEU&_?#grO9Dq>%AY=>~^=9ma&AO2})?{->6&sEBWLmhRO4hpPj`{%<(;K*>Oc)W_P)h5_Nm=v&#&{4%MGMBujA4u?an$3?pSC;~q8P{7%|>O~Lm2ub!nh8Hc_HVZBd*&R!48f8d>PyXhZk1qxUvYqJv46(wcOqe!EM$ZafxOZO&`rp)tJ)Nyt->jqMOv@M?#<cYIjAYAV~}=K}6F#%E9baD*#vtr#G+VgJzw^?-8Pb)mw-b+sN1#IQj|3P)QtMSHjq9o55i2wcSgxBUJs-v`TA~@X^r<o*b<Ouv6$`Mx8>pb;oBLC}k5oN+U^MK`JcZh2%FpLat9xh!O88+U5t&Td5TPUxpZze{z+;oFOiYGsA^Twbn}B3T!(w2yB~Gw_2T{%D|fbNq+KA(Kth_DQ~V+?n^)^LstdcSk-vu;OS_@lAOY~5eC(v>=Q2!F4<=<E5e6p23G3q%>)t7XJYe=aQQl|+$D@5>u{$ACo?&GIfu4{3F3R<vhFnN@6>f0t0S<)+@4Y;wO11VZLmSLsN~ye382biP7By|0&pY`8o<8I5bV>sOLEGWWCl<q<8`YPW>PS9?Y(@3wP$C{322nvAGM&2u{e2*USjzd`RiCBq53-Pu{19ntE=l)k#aR?RX=7#tJ+(ibDh*?G;qo~bW4xnaMB@-nG;!;y2uzNvJ$(u@EAl<BD?%Y-Wq5rf;v**73xT{W_EohlY^5H>LT@Ka1USBZRR6MlKBpkMk<>U&C6V4;(_-BzKRPCWhjS^=5yz_A{o$gn&y>8l_)H7bnc)tZc~O1BYBw#A{i^0&GVQ<MS@J9<&m{*z?6BW^izs`ix_@g8?ReMrC+ek59mQ0bMaq>J0iG46K9#>j&lcoL(H6t>CGU)y{#M8Tz%l6DmFJduF$MnSE<K#Y+?feO3CgK$mh;)bzD$-A}I&k5+~QyJgc`<>grmi$4K^OAliFdH<I=xp9t1FYO#&dbbbkrJx3fmvGmPI9Z}^6y>qv>f(NQhB{K}G;&NS`+q3B)w55Z$1WE67W+1(DTQ`Ak?Xa|tT@lKIPOnte_7S}rlI9%|k?-$MMrZA{^IKaP96~bC+UZ$VR|a}_k}Wdq^!%7nr{`&nTT(yaa6D^6B+Fq*#sykg2&dviITO9?B3<7{x;cQL=-T<MprRh5+WNPGT36?@m5xs8Y-#j%X5gN;brTfvNstZ&(aX0xAj!GSd-sCqshyi3Q5l*50S)f8+gs5??L{R|PV3=XR&!j@lapf6Ve~FDLG%vmP85O@MN`f`HHDxLda2Ry^yozu**u+c|61ds2l5Zs&TmB$REH>*WP7bB#OTwCWaSmoak#ff@-{;xiBHn_XFn7%nW>A-56sjh&~!7B?6&lper%(UpmuQW{8l7UZ74CPW%9SK=2;4hkC(G#X$^K}aH8JU&BGeIoUFWqPkEdVlH|hFSHzWMeAk$pWgtogZ!_1<Z$%Py7CDYU_q?nws*!xSl@2^K1CqBHB1uPkn4`_<O6p7`$)^d)u$-<AzpU{v&(i)IGkWgvHSIft4>p)c#J>CqWqC0ztHDj>0Fr}LD2~r5A22&JNFwEAByyYAmXhpwHV;@HMRdxEJF6*|Lx#2;)x9`C3aA`#?fh1^1cjaQ-NdyOrLN8uS!zs}VH^z{ki5*mEidctWt7EfFp+d(F~+kHr3KWULLqF2xD(W^L)#dh>)1;^f2%p^xA9I0Zvlh)WmPmSc;Cur4&r^A3E_PzpKZ!N%)t(gI831t$Gm{dfqq@X?%MgSo|_SpA%P3EtS%bpaz(LNTk<k9)Rt`4uvH_hKUfcEu*_%hgrg7^sN0f_cb|oBN&oqh(WyM+N%Obs>jKI_H49kjH|y$5ze)bkq@OxaK49MFh`_w9oB0Q(m`t~<-{Tk^HCo~x&JtbmmSMb)bD@^mwew5eQV7klP*gC;QdYx?WL!?r0m;iO5Xm?TPa1Kqnn0>dQLbBXlo)<eIQ!b}t>7^NFZi{-URQly!OIJi)BxUP#^4>+onCKA7tJ9SuDtF8d@k{lr05W}8lpM<0lRXZe0F{-l4zVvGsP9jvYO@$MUQ5IY&6jKW)SDx)}5wRXp~V<G$+I$qd26S9nl5aK3H<kL7qGkCj+}!S!fg=>fle*O2U=CjWL`z6I#pdb#-RTpkrLcMN7r^HbWKP+Zr|kpgA}-ReUw+DBT;yuX??hq0tZH5d=kgqrBNg;jEduwe9*?%`1HA%pOtd*_lD=d0S;C!py-GE6a^cvV%|JEKrtptAWIQ$m5{MXWRI#IHF=u`LPFyNDf44#j(*J5Ikx-GeI2l(OsW6M{RCIQHGfID2rbFX6_AfRUmqkQdU>*-c6u<AlSz0pYKMq6MuVNNzgP}ZA#~Ls*n~^d5UNMG;ZAM&A?Z0>jq;|QmbpE*ktG$DPgXgW+bRiVC|eW^o<7&75uK9-|DRiBH7h0`&I7{l1x@$)S1aZl4<**LfVQv7|B6;r7k3yUQ{ZN^lr?T`=H06HtK8Rw*rZT>{(O+kfK2rAjv&MxSbgSd0Nx+^+{?8ARRVN6OduXu@yjmMi~44tZ}|d{`~T)-)5)rNX%Y1uK7w`U8;HM2uFjV4A7eanwRyN`K^4Vb@;<VGdLcCA*zE!Gn|or7R__Fw-R3<m3k>Z{Oh!KSyi{9=MM(kV4-)JF?xO`$A#!+i?{Ev#nBgYD>C%xWhFylx1ZjPs911q_g45o*+)F<+RR>8m$&J3^rHu>%b+uR2wyz&l#TO=tx6vT9b_8)P-Y+ldH(e`Ftp9z#~YXz1b6fb;4Z6qfxA9qX&v~*-g=v%x1QEb-GENMe+akdG@iA-31bbxELjw9qslo#_T28R-U`Y-CBOk(edVlman3jMF)yDDfvb-h30yVnRCIvwNwB?D!Z?diQWHN%KIM|f9S73GC+OYs+U~9JfwE8eMq2OU2w#aQoUHKqLq%0A`1~xgI>6_TasfR*kd!>oWT+{&k)fu9FEix@@$bS=bM5?ABoWh4Nf~2Yk@PMBM+^rGO}3Cc%uqA%W8J)#5anbddGaKDkmUAf?~$z942JO0Al-@JyFP1Ni=0Y8?Az}Wzg^<-sKgUW47A>gLE|iqO!=Zn^cI@UtOlBu#$?-1K;#pfX!<ae@rV}wFCyo+{9mDWOgP7P?fh21L?xccv_3(X)pfQcf0}qOlBd}?Vb2~Pw=1j%O?E+6tZ>%l<bpX+U0+MOrak;{61s<z=p%eMS+}t`ZaD-l-)+j`Q~vray+=i%RIAP_ZCLhkCXjb0RjtEPk)0Xnu-w+ooM!p!i&=}0I&%2A*ea7f$mQ$yyE#qnI&Kr8FzMR)twf9tBbf;xsf)1k4Mu!MGCm=>8<j7A{pf8Z)lW#~6-h@MRw5dP=^YA_&^A9XR;~U&N^H;u8P$!7U*@G7v#cg^E6Dkj^b`#8W%dx{d=9ESpVTP?(_K^Wqr0X!6PdWg7SU#3_KVO*ug_X=N(huAB`E<IeH~4tYG3{evTC8b<mb$W=@Q_8)49DFpxFk<cq0c;LKhe9#MH$pF?DHXLZFry0WkZ=Pk7*7AN>Jkq)@i|Hke4*p8BEjesz)5q1s>|&XVfw%ml$ath+wGavBEv{LN#dv@3zr?3yubU$w_H2Qo8WJHPcNMbh@{d|LVwj#YqUp7|9=B=aeM1XMaY>vMlXi5<;wIOWkTD+=>Xn#v*g>wK!$&TmB$N!xSz30*~(nO3>70}Phh({3EFml+?h$4I>kJ0#l_6h>0X8dfP+De(-dZ2S9M?gL$q-80ae8OM>jeI4{)-LtOF?#aiss8*ocnZZHZS)XH|2WlBNQ_HwU?$!fYlI$H@9}lAGApmN|bK|$-h_vm?A-f7^msKLa;7G1gK4IqZ-VAZ9RvK#hP(mk(F+P9TG(+dCHsx8%HRkR7HHE&~#8B7u-1)7bBBE-aS=SS{tcKM&E3-i|xN$#bsG@yacfx|1j4br9>8PWfdF&Rd5ad}R^xr?)fVVM*k8-RuL&xvgy&!J^MI#@wU%BS%s-Hbqu4ok(jXE>%T=|5$&z`H%mXFjpiT7%f*<IE5)4aSb8fA`1H~rlCt<nz;@yhp#Y~3o)HGDn>a)}<0yv!KMJiokQB!jn+3_Kwji5Da_Z1@#R<kSz`LmlRG=eHtBP$7W40Eqx$LP)Y&%DkP~8%XlO1NN*08lsJ4h!c__#R<qDiGQjvbno_7@K7Nr9yE>%cs|H_pwgkJJB%*$;GJecx8`l#sc45F=B*i;$W8}d<2xGAqkgmqzmX;$bRAIqbME|BBvBP8s-Oc^{ZfT*bJBj)X`PU4X6S~ebr=5x|A3^QkyKuQjA5(d+TV;I%e*&!D~_lJlt|b4>bg45SF=CcesHV8ZHBE1A1m)d9I@kAl_`!@zlhoUobwLI!Dp3_N1i*s6-iVBN|7T#GA@%S6%0L@g$*S2)B?mv>QQ758;?xM1Tv*jus&s<jrZ&$JH#*!=W2kF>bmh;5k&={@`D`K9J8*5714ta$Ou|HGYDF5EA7r$HX6bX8>v+|2trFHlJi#od`Kn*s%D=%zZKL7L8Z0FwyuV>g4*O*vjys9#-PUY-SasU3GI0cgW3|k4#}*ul6}yjXoo$u?<N9-7N4EpiX<ul<pLRSPl_=jB&)7tw2^FP3rY5y*@goxjy94mPDt{@ksH()xaR&-D$pm-&ssA_zBqsTtsEHP+s#4&X_h(GwPJg$uE-MGr0%_0&{cX`WrM7@%RauJII91Dds81Z&&lmPv?nHZo>542?)+AzNF-n8l-KE_x*F$NfgU`Uam+$KINoMhq}5ZKj(LY8nfIrsNxHs`J`6Ypqf`(FLViJk(DhkUrM~->zuko!31L~jlDuM>{VcRl;qWKL3_5K)GdOK8>vQjB{YIM%DFjoJo4k@1ujJgvrkJWf_XK;4a0R<qf=A^CO7i)8`1Yu01l1uWQeEA(ifQB(Vm)RU5<GT0vp{z(RvDhrFvKy732}^K#k-Pqj(0_H4l|Ao!s5`m+gs0FK|lt;rC3*I#q)|qJO?)oUybuz=Ky50=AEw!nHQ*V9&5BBb2KEh<!p1gFV})u!F%Jk;^+~MnaHg;W|~zsDirAtoNHIKaep>58TV(i?#i{&<UBBrfuiY<AwLAWQeWtIZA0Aa8U+;hf^Do(P^ooeE$&WIX=c9-y+Y`D9eO!d!C8Ft&>GJyo!LVd&T|rLoW+i9=%;#8V|gDiR*k#o?H!@!z42RJ7}SuIQ@eUb9Lws`tr-sS3|~hV(4E?u!JT?rcXfK<hm$x=2Qkxf5^LDPIOu3+=&Eh!x8fL)jCN(UU2v?ce#No%NaLgUHp8R1vp)NyvxpQ9TO6-c*WO5bB%S(g0dP3A8TOl;JHHjlgpl;kufb7Soe51U82(I34UfFc*dzVXIYgUFl{K1)47Y&%gCvJAU!OdrDP+uv_N97{d=hM9jf|G~s>-q@mim_S?arZyDoDQES3vf@ti~1OgN4s%t-j64aMyIc$vszjCCWo8Gu0j>rlt0vQMT=2GIYtqgEzie2OyyuQVG;nQiZY_kh|vUaYik$Gh=sc6o0$UA#W+m9c6|*RgriJb}N&~E|=*KUV;xVU>l3WFKRhqqEN0fQvX5D>P_)LB;gqeD0oJi8X25k6M=9h_+v&h!5^z`^jalnQ@=Bhg4s<1r&sSRiOyE8qxa+Fp+u{9-_KM{2$4b=cZ^G)#WWwbPOmq!XhGP_$$+puscohoRK=oTEG97}cV_|(_5B;EG2`32Y9fYcuG?6Nsis~NK);MMqOY#QaVrmbq7`Z4qlv+C*3)b_>uKGn0ch)z!AMgc(G^<Y?VVL{;MBt#5G&ld@k^Z*vQxYDLghqrZLo5i9C}s^n;LpE(8q4p@B_|j63J`x$PU6d4JQ@+-S6R{ZR3|h2pOuyuc9246dggxTu~4U!e;gcg!Nf`GXbitQ4>NK#v^tiLn&W<+>@d7AmbbJ4O}QDf{3E1{@|`0sm=ie<qtx}NK$XcPS~s)xnxZ?)l{w9jc%A0s;QE}?y07J0LAnMswopj#7&gaI-pV(J%BMDJX6tv@iJo=<H<AiI3cJl=f#eg$Llc8w_)fL3gb3(BSmUPj<{}P3?L0}-nB{cyXYo3>#$1Dl|_K=A)af<?)GMgZnN%)PBgn{I%#&jVD3PNp?US!l=^?&|7_@!U+u4`E+mOTS%_$QR5_TyYB>O_==A2bme8!zv_3*KuzCy8Vspc>8#ww2#!yKguzGS|xzz~fc(3hViXEX+kfv3ZqlAx+R`BF#KY-0dA2Vtux~)4d+(0Rt;87Yw*Ko4@4d8|3cf3RWxF9(ooP&1xf%7&={x-m%5|mlol9`mnnGVCHT6QIGg|(d-gtg78TgA^%#bizIY~M=fouY9DSX18IsNA!TrwAPtY-6={lB<v495sci4fbt_L8T~{Ki=~Al705FLVO5rV3E(>Oc3IH<~7d{m+#X|T@H<FTq+7qW?uPn3~lKZWK`<?^jh^->bi~9GZ>tgeEyy}d>d_0Evn6sO8`|Cb6U`@1A`;k(17-BhG?JGU6w;)Ubn?IUbjkNCJ|HD9?w@?W14c<8P)5P7L+k22_*~(u4*;KZ$pU$@5}eRcH1ATtLs*=b2V64KW4<j+FPG<oz%uSAef0rtSWEPfrDsHWCztjn8-@cb`$+)p0-b9m;cCH<19r`M+&?`9ch-{u1{uiP&h(OrQQrK;>)^Ce5CbM<2RX@Et=L6*UMaP;(;&)K9<YKZk~`opF6)5)PP>uG_M4#^~9N*d)+~V+{P0f2K6!%1T|I`oadQ}iUgVN%PyObG|V#_pi+r<cFF8`^CHPCw9OAlN%%T){AIW!f;%*cml^Ijx9c~=%n_R23{u_Oy5ZZ^2lA@ol*#mAp;-n`IcyopsCVJy2;qJ1{8k?Z^(c~Z6fV(oUCpx`Oy$3>wR;R|Zw4yBw^g4(^&-J~Y4ueisz=f<A+zU*<0;m@U5P>3dv5kt>_Fv-{7EY_Vpmq@UT!)_ap|BwK~g@Q8A$ou)=e;6TQIEySWfxc8@sBPGu4YrK+E~pg$l2o-`eZo5R!q`R?tfPzp&W5ldq9sE9l3JT0u{1-0}hnhr>l6l3Ze8qb^*D45yMsITHIpL>&GG9fY<(_<QaAR!~tBQtb^~L9MIv(Mm_Fb+$BrJ2P<4+qwyl_#|0}0_zvm?=7&Nlb;3FQzHom*-L-6q9>edx3{8)`j2XB+j_W`)f`v!<YZ}d7`@9(5WT~?6PDma(dud_HW{AcI5j4p9=)g{oA*>!?OPuhh=*P~zZFSPF``&9@U_fP&Y83#S*eM1oC)rcyv-0v;*)s(*$+#IV^s)?jjakHOL8-)?6zc`e$+)%AY*v#{8ms=?I<y)Wp=o(=2=;bkB7Bn$ryHKaJb&q&AVI4FO!vw6q_HIbeUY3+K#w#mhWPpq0f`xL3{4}RwPm5k>dy?)ywK4NXdtX>A-3;AbFc1l5}*BIocewq|QXLgk%$vVL50Wet6@Ng)@P7nd_sk8j`TV1SO&Rz9q|pYFQ0#su7SJ6h(1-Zux-OnL#=!C%=)~yvA@jPT4$bxt{@^&RUs8o6C7a+eYl3pEG$>Be-^ctA~QZ57&X#<;|t8&c$156q{k3C>%h&%)mo0>+Th`{P=4yL3Jh-JquA<K<)V$aIau5!V_~spn`3TCvz_(k+rfz<=PDNZM+l0TfnS;SrttS-nXWjgLvO&LU^B=YMYu6bFc%ZlBo`IAmyhrFCcTET|;Q>`K;Om;sSEaFRl=AMo5MP?$)xpD6PvC+hWzq%gj)<vRT7c=dk`@JsiiQL|PkG?{O5u(sw_g{d<4ba7#*`UsCR{Z;wfUA`#cR*4m<7R%c#M@-?Y+Q@6?o%-b9hn74H^DZvzz>6XQxMz^FCXSXbY)^2W%`kXaoB)@d+{8G0RLK80(6^y);)vzKNmlJeA@-hoVGR~5gM#!rskW*cVn#l8jqeLvWp2sH>n6Cu)X7Ct+7yMdvud6<<;N^vuY5?ytWAF~^P6@bVj^?;av5g~3>=G~eiViQCI0PCvS78m|-1)6YqQNuG6jvn6YMPT3J^Bi=(QDtEL7a12cVbzgu|+{Oo)Eo^vXgFhL>FlLipjlnOF}m#+Qn)P%%Q{*o(>|n@b%F`IGA?6J57^mLNmt(9fK_{TJpfR8Oj6S*050)&B3WD53HkXcR4@BuR_3k_1_#aRR4|gW)Fo!Y3kPg>|-^r@TD_(M44x22ASt=m7TgX2U7$sb5C|t7bMQYXSEe?6Y==|ForVS=f-cv5tW6?k3Gmkav(}8j*T>d;7Qw=3F4TKHvGIfX)9N*t<tSLx#8R|;;QoWE~6Z#)3Aaa*v80D0=wejL6u_3c)xzsqPaDu#@ji~AJRfrPx0)u#w~um8TjgL-C$e-SG77xij9U&64KVI+bhD>>dekr`@}OPRPVcXeyg`8h-BB!)~|YpkYuvrqRvbPl1$sT71Bm8gpmx+k4Pq&-d3ny7aMP79tagOM6&JtRwOe*Qd9wwqCplU$vs54of#r|TGR9WSv=C4D$P2pH0xY~Cv63g)ep6wEDQ?K^Tbcubv{Z+o~cqvbNTWAGRXoc0M+)lHD9T#OEoVY@n|q~26{6<^Rhk@#+9$MxSkaa)yr>)hNuo5&2UEgSv1ex-b#@{2)**dzYcbnRdp+R{vg2(7J8Q%qvvOSU5H+`=>86yj=q>%k)cN~E8#|2*Uxk-Di&PZy%j!C_7TszHoMo=<(WDi{p!JLIOxnC!WYj(W#fEe3*HAdyPs>@`UGSk&%gc#hPL_pcmwl-;ErAa++{T{aMx!nt)s!%TW>S;*3-Hv9MCBq0O9s6EB#>Xo8s0G&5{wqj<FF+Q?Bja>aC#eQvw`7@mJ1T7w3dCACvXjP|o_8k#bhEPE`j8p9I@mMbmiLO1mg}_{t@ZI}Qd8WJ2$j*LH7(57d1ml)u>0-BniE$qJu8<XOdn&(G?u1AP9d7tr$qNl64vGMmbSsv~?0IR^3XLS}RA{8l6pDN)H8V_cE+E&)dj2VqUNkUY##jPPUKyp|B<WFmQTUwn|{_Gj;rr0B3FsL;q{1K_(pYg~&leSYP7Tz?r(ROAUI2wJbjpm7#mrhHK=dJE2GRs+t;c(QFMpmO4|38(i%A&+R`3ZwC53&%dGZw-Nie%H=#bxc&`iA?JobXi?TPV%RV2P1i!jU)DK@^QP;dP_Nsk}M@-<izFVf;oI$-%Gl#J^XZ%x~G%K%BWDgZewxds(8ug?=0V*yZ5Lnlxp31r5G!*_)I+SPKsNHMJPKn&~mx0n?cU<*B7%IU+&Ej@7O%B2f6$x``z@W??;R>ij%IL-^$tOFp`-7lDbGX-(bXNB;ym3yRrK6*N@&rQpHb5<`v04(KN_6`{Nitkb!M}VD4IT{5Io>qQX(}%e=H>meoXV1v#Gzp@Koa%pQWA&-s<-lRAZ9x@!tNx@(Fv=SdabF-SRSXfi>_UZ1t#EJv=_5OWnM<!>Lp3hGED@MT$5)?M;*YQuC1cfg_D-VD%eb7j1d5h$UHi*{z}!jzf1G*cx|%Zvb+I!LylsOkLZ4@fGNeE!>5f+CSG34^Tq+;k07j##EjYG4Tec4mTD9@bqSpNl!z$8R1RrCoAi_RJXe&)WUz>za(;;-qcow_c>6UgX*Nv@|Fjs{qM7Gck@x=2JBZh<kL_=LUt8O|LAO^XQe87ihjnQ>66k)T`IdZ$(lOk|k?jC7jFZqH=bC!BTtLjq~*~<MZ_xw0B{LWE+UWNcwCItCX9Rcm`GWEG5((yNM&bLj%2;n;cY;d>#K^-LtOF?#aiks8*cYnZZHZS)W6o2kII3Kt1Dxr^=u!>GqD}Z<t<5-h;`(bK|!{3MxreS#cHQE~`X-fs|Zjhr)d2y%|DUtu&PPp$JbBW4`{dNw$P2lk%(#8}oK5n|g}X>>^T$J$HU9sEFOVB<s2!zGXG6ep;yyl0m5ZF++9k+q#n`%w*)Fhpo>?>jICRLluHNtBn5pryTG$Mh#d(6d$VWLaa|%-+mfZP(CVuJijv3*Hu4zu3UjDE+2Jf;JNaNdY?U4qb)$G!8nYoEy?V;>iZdA-d2*bQzYSi?)+Bm2#0v->s$OvOz(Iu?DH|EOY{KhWyYZ9`8^JU8vFyOZWN=`NW37aVJBhedOiit$aBw~--;wbg#q#cBnE^DA<1gV{B~w<Ajt=3*t4u?h&GZTPDq9nCm@3){;6uwz1v&CL)D?^1L+E$53-)Ebjb4#6Ae9hr&-VidRuq$+#!g0fd<Xv1sWPR(tsX$zqvrmAE)~_VnCtLx$|3*M75!)f(}&mOKHAMPy12ybwaY4p&OpoT__a%1ClZ$sk{Igx991|a1x<M>YI;KR2fR7>(q5!ou{tZpUptHf#Ej828NH7chQd6A+3sdGurWa77neRxx=)d7tEZIbDlfD6;xCtN|7S~H7>I%6&F3Ji49QoloiCF>QRIb8|O^P#59c;WXe7p&)rALh*2Cq<6+FYZv0k2QGuxZ9EUaWtgB%K^q>wh!q?6W!q?kMyVI79hOy%)&1!jR5L$AQoYR&Wtj{n>;1$|-ek-UEf=X)-Ze0y&1+}TXW((BIj6sd(v*>e9658t*1~t)ih30YI;kf)f?I!L0ptBU<9h%=f`w&oFC>O|pds369S&*y>k<mu7nJpyQZ)P73v^d&1Ns1Ga{BYz3H72rs|0yBpljmoRVo*v{JiGmVv~+wKS)_jESl2S{vAQBlY?H$GW<fXUX_XDK-mV3C9zE2zC-+fGy?+%mChdE<=hl!s<9hD=R?$f$gyxjj>7=?E=c<7ol$deMM?N^-W>~q^Q{|3%ha;Kyr>F7GJVzr29D`9b=!I5lS0MoJ`mEL9mp(Px#QED@x{(l=<vYnMnAy)t4HXZ6a?qgTwljm{_Od?rcGj=7Nt!}5<(yvpz)6c+mXp+P+)^mIiGpti7VKhR?6Y#Yuq370*ZuV>d<2yuB~o1-w#sqj6=OZ791=WuJF`HCEmj#`*D%I0i;c0074b^eIpP%|Tqqg_3t^$?-0iI=uOJ`;U|X!KvwC{PBA%n2hO@?b@^b*PS@X_Wh0H5dT2-e*Pv&??XwT}_3&AG97=Lg4RvbOTF%!8J$4s*tM};E&fpcw}Hg3>nCgTQe)?MjVnhXfWQFAmMGUSJFx2i#I8;iJCM)DrPyKZ9*i~11i+Z4s4r26%<SRwShPQ@In;4D6xY>g+E&g`KJ=Q)iv&SJ+V^sRfh!#nDXu?|y(KSjBDZ~Rsl22~{G)UKWp$FjQgbB057!`Imcbf<P^aHrnZUCmzj;UxCcLCmzA#2U6R_E&0*&{f;cZ^bbpN$$#fyWm(?{fc92qQ*z@ZH7m2XMOfZXHhL2HXX0jlYO{H(y8B-00(i%>|h(px$|3*ObAKu{2CmU)tU07g5l4!)bPm5j6KpH4MnuMR9T~$$*@YWE>#X;zDb$qI~WX=_BABWDB=a%So5WIyq|p{c2dW0zYI63Ci!x6t&Uq);|ljd*=Mw2-{xdEZaQD;o-4i*#ZGD(%VK|#rlk&{QMUbKgL+bcM{s<zW<WyqB<}$MLs<>TadY)Oqb}H)vEw#Az+FF)x3uYwibFKhQ_o8ToYOwR34MYiI8$A=vAF-rU0;3x+VAyS{kQ*J{)GShum4j1hX3;Czy9n0{eOS`$G`rce_{>gU;i!8ZzW>>lQRD${!`NMmp_%i+#jn3vHbBjr9b`l+n@hLKK|v;zd2w2Y=8VqN`L+>{?WNVmCfnT|LNcUKW}wn*Z'


def iso(value):
    return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')

def when(value):
    return value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(UTC)

def sha(data):
    return hashlib.sha256(data).hexdigest()

def file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()

def code_hash():
    return file_sha(Path(__file__))

def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n', encoding='utf-8')

class CSVFile:
    """Streaming outputs keep memory bounded for full configuration ledgers."""
    def __init__(self, path, fields):
        self.file = path.open('w', newline='', encoding='utf-8')
        self.writer = csv.DictWriter(self.file, fieldnames=fields, extrasaction='raise')
        self.writer.writeheader()
        self.count = 0

    def add(self, row):
        self.writer.writerow(row)
        self.count += 1

    def close(self):
        self.file.close()

def write_csv(path, rows, fields=None):
    """Stream rows; never materialize the full multi-decade source or ledgers."""
    iterator = iter(rows)
    first = next(iterator, None)
    if fields is None:
        fields = list(first) if first is not None else []
    sink = CSVFile(path, fields)
    try:
        if first is not None:
            sink.add(first)
        for row in iterator:
            sink.add(row)
    finally:
        sink.close()
    return sink.count

def set_status(**values):
    with LOCK:
        STATUS.update(values, updated_at=iso(datetime.now(UTC)))
        if RUN_CLOCK is not None:
            STATUS['elapsed_seconds'] = round(time.monotonic() - RUN_CLOCK, 1)
        OUT.mkdir(parents=True, exist_ok=True)
        pending = OUT / f'.status-{os.getpid()}.tmp'
        write_json(pending, STATUS)
        os.replace(pending, OUT / 'status.json')
    print(json.dumps({k: STATUS[k] for k in ('state', 'progress', 'message')}), flush=True)

def read_status():
    try:
        data = json.loads((OUT / 'status.json').read_text())
        if data.get('version') == VERSION:
            return data
    except (OSError, ValueError):
        pass
    with LOCK:
        return dict(STATUS)

def atr14(bars):
    out = [None] * len(bars)
    tr = [None] * len(bars)
    for i in range(1, len(bars)):
        tr[i] = max(bars[i][2] - bars[i][3], abs(bars[i][2] - bars[i-1][4]), abs(bars[i][3] - bars[i-1][4]))
    if len(bars) > 14:
        out[14] = sum(tr[1:15]) / 14
        for i in range(15, len(bars)):
            out[i] = (out[i-1] * 13 + tr[i]) / 14
    return out

def previous_high(highs, lookback):
    """Monotonic queue; excludes the current signal candle."""
    out, queue = [None] * len(highs), deque()
    for i in range(len(highs)):
        while queue and queue[0] < i - lookback:
            queue.popleft()
        if i >= lookback:
            out[i] = highs[queue[0]]
        while queue and highs[queue[-1]] <= highs[i]:
            queue.pop()
        queue.append(i)
    return out

def bearish_engulf(bars, i):
    if i < 1:
        return False
    po, pc, op, cl = bars[i-1][1], bars[i-1][4], bars[i][1], bars[i][4]
    return pc > po and cl < op and op >= pc and cl <= po

def find_paths(bars, i, stop_override=None):
    """Fixed stop/target first-touch is reusable; eligibility is replayed per cost."""
    reference = bars[i][4]
    stop = bars[i][2] + 10*TICK if stop_override is None else stop_override
    target = reference - RR*(stop-reference)
    common = dict(signal_index=i, signal=iso(bars[i][0]), entry=iso(bars[i][0]+BAR),
                  reference_entry=reference, stop=stop, target=target,
                  next_open=bars[i+1][1] if i+1 < len(bars) else None,
                  next_candle_delay_hours=(bars[i+1][0]-bars[i][0]-BAR).total_seconds()/3600 if i+1 < len(bars) else None)
    for j in range(i+1, len(bars)):
        op, hi, lo = bars[j][1:4]
        hit_stop, hit_target = hi >= stop, lo <= target
        if not hit_stop and not hit_target:
            continue
        both = hit_stop and hit_target
        if both:
            legacy_reason = 'TARGET' if (op-lo) < (hi-op) else 'STOP'
        else:
            legacy_reason = 'STOP' if hit_stop else 'TARGET'
        legacy = dict(common, exit_index=j, exit=iso(bars[j][0]+BAR),
                      exit_price=stop if legacy_reason == 'STOP' else target,
                      reason=legacy_reason, dual_touch=int(both), gap_stop=0, gap_target=0)
        # Opening prices are observed MID prints, not guaranteed broker fills.
        if op >= stop:
            reason, price, exit_time = 'STOP_GAP' if op > stop else 'STOP', max(op, stop), bars[j][0]
        elif op <= target:
            reason, price, exit_time = 'TARGET_GAP_CAPPED', target, bars[j][0]
        elif hit_stop:
            reason, price, exit_time = 'STOP', stop, bars[j][0]+BAR
        else:
            reason, price, exit_time = 'TARGET', target, bars[j][0]+BAR
        stress = dict(common, exit_index=j, exit=iso(exit_time), exit_price=price, reason=reason,
                      dual_touch=int(both), gap_stop=int(op > stop), gap_target=int(op <= target))
        return {'NEAREST_OPEN_SENSITIVITY': legacy, 'STOP_FIRST_GAP_STRESS': stress}
    opened = dict(common, exit_index=None, exit=None, exit_price=None,
                  reason='OPEN_AT_DATA_END', dual_touch=0, gap_stop=0, gap_target=0)
    return {model: dict(opened) for model in MODELS}

def geometry(path, cost):
    fill = path['reference_entry'] - cost*TICK
    if when(path['entry']) >= END:
        return None, 'ENTRY_AT_OR_AFTER_CUTOFF'
    if not 0 < path['target'] < fill < path['stop']:
        return None, 'INVALID_FILL_STOP_TARGET_GEOMETRY'
    return fill, None

def r_value(path, cost):
    fill, problem = geometry(path, cost)
    if problem or path['exit'] is None:
        return None
    if path['reason'] == 'STOP':
        return -1.0  # preserve archive exact -1 convention
    return (fill-path['exit_price'])/(path['stop']-fill)

def replay(indices, paths, model, cost):
    """p0 per exact geometry and assumed cost, including unresolved positions."""
    accepted, invalid, blocked = [], [], 0
    occupied_until = -1
    for i in indices:
        if i < occupied_until:
            blocked += 1
            continue
        path = paths[i][model]
        _, problem = geometry(path, cost)
        if problem:
            invalid.append((i, problem))
            continue
        accepted.append(i)
        occupied_until = path['exit_index'] if path['exit_index'] is not None else math.inf
    return accepted, invalid, blocked

def source_hash(bars):
    return sha('\n'.join(f'{iso(t)},{op:.5f},{hi:.5f},{lo:.5f},{cl:.5f}' for t,op,hi,lo,cl in bars).encode())

def signal_hash(indices, bars):
    return sha('\n'.join(iso(bars[i][0]) for i in indices).encode())

def validate_bars(bars, step=BAR):
    if not bars:
        raise RuntimeError('Empty dataset.')
    seconds=int(step.total_seconds())
    for i,(t,op,hi,lo,cl) in enumerate(bars):
        if t.tzinfo is None or t.second or t.microsecond or int(t.timestamp()) % seconds:
            raise RuntimeError('Candle timestamp is not aligned to the requested granularity.')
        if not all(math.isfinite(x) and x > 0 for x in (op,hi,lo,cl)) or not lo <= min(op,cl) <= max(op,cl) <= hi:
            raise RuntimeError('Invalid OHLC at '+iso(t))
        if i and t <= bars[i-1][0]:
            raise RuntimeError('Unsorted or duplicate candle timestamp.')
        if not START <= t < END:
            raise RuntimeError('Candle outside the frozen study window.')

def crosscheck_history(work, bars, volumes, h1, h1_volumes):
    """All observed H1 bars must match grouped native M15 OHLC and price counts.

    A sparse M15 hour is accepted only when its observed bars exhaust the H1
    price count and reproduce its OHLC. No missing candle is manufactured.
    """
    grouped=defaultdict(list)
    for bar in bars:
        grouped[bar[0].replace(minute=0)].append(bar)
    controls=[]
    def check(name,actual,expected):
        controls.append(dict(check=name,status='PASS' if actual==expected else 'FAIL',actual=actual,expected=expected))
    old=[bar for bar in h1 if bar[0]<PINNED_H1_END]
    check('historical_H1_count_matches_prior_research',len(old),PINNED_H1_COUNT)
    check('historical_H1_OHLC_hash_matches_prior_research',source_hash(old),PINNED_H1_SHA256)
    h1_times={b[0] for b in h1}
    check('native_M15_hour_set_equals_H1_hour_set',set(grouped)==h1_times,True)
    mismatch,sparse=0,0
    out=CSVFile(work/'h1_m15_crosschecks.csv',['hour','m15_bars','m15_price_count','h1_price_count','max_ohlc_error_ticks','status'])
    try:
        for t,op,hi,lo,cl in h1:
            rows=grouped.get(t,[])
            if not rows:
                count,error=0,None
                okay=False
            else:
                aggregated=(rows[0][1],max(r[2] for r in rows),min(r[3] for r in rows),rows[-1][4])
                error=max(abs(x-y)/TICK for x,y in zip(aggregated,(op,hi,lo,cl)))
                count=sum(volumes[r[0]] for r in rows)
                okay=len(rows)<=4 and error<=1e-6 and count==h1_volumes[t]
            mismatch+=not okay
            sparse+=bool(rows) and len(rows)<4
            out.add(dict(hour=iso(t),m15_bars=len(rows),m15_price_count=count,h1_price_count=h1_volumes[t],
                         max_ohlc_error_ticks=error,status='PASS_NATIVE_SPARSE_PRICE_INTERVALS' if okay and len(rows)<4 else ('PASS' if okay else 'FAIL')))
    finally:
        out.close()
    check('all_H1_native_M15_aggregation_mismatches',mismatch,0)
    check('first_M15_in_first_requested_week',START <= bars[0][0]<START+timedelta(days=7),True)
    check('last_M15_in_final_requested_day',END-timedelta(days=1)<=bars[-1][0]<END,True)
    annual=[]
    for y in range(START.year,END.year+1):
        subset=[b for b in bars if b[0].year==y]
        annual.append(dict(year=y,candles=len(subset),first=iso(subset[0][0]) if subset else None,
                           last=iso(subset[-1][0]) if subset else None,partial_year=int(y==END.year),
                           first_month=subset[0][0].month if subset else None,last_month=subset[-1][0].month if subset else None))
    write_csv(work/'yearly_data_coverage.csv',annual)
    check('every_requested_year_contains_candles',all(r['candles']>0 for r in annual),True)
    check('complete_years_reach_January_and_December',all(r['first_month']==1 and r['last_month']==12 for r in annual if not r['partial_year']),True)
    write_csv(work/'source_controls.csv',controls)
    write_json(work/'coverage_crosscheck_summary.json',dict(h1_hours=len(h1),native_m15_candles=len(bars),
               aggregation_mismatches=mismatch,sparse_hours_verified_by_price_count=sparse,synthetic_candles=0,
               h1_source_sha256=source_hash(h1),native_m15_source_sha256=source_hash(bars)))
    if any(row['status']!='PASS' for row in controls):
        raise RuntimeError('Full-history source/aggregation controls failed; inspect source_controls.csv and crosschecks. No discovery results are valid.')
    return controls

def stats(values):
    values = list(values)
    wins = [x for x in values if x > 0]
    losses = [x for x in values if x < 0]
    total = peak = dd = 0.
    streak = worst_streak = 0
    for x in values:
        total += x
        peak = max(peak,total)
        dd = min(dd,total-peak)
        streak = streak+1 if x < 0 else 0
        worst_streak = max(worst_streak,streak)
    return dict(closed_trades=len(values), wins=len(wins), losses=len(losses),
                win_rate_pct=100*len(wins)/len(values) if values else None,
                total_r=total, expectancy_r=total/len(values) if values else None,
                profit_factor=sum(wins)/-sum(losses) if losses else None,
                max_closed_dd_r=dd, max_losing_streak=worst_streak)

def quantile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    index = (len(values)-1)*fraction
    lo, hi = math.floor(index), math.ceil(index)
    return values[lo] + (values[hi]-values[lo])*(index-lo)

def month_bounds():
    result = []
    year, month = START.year, START.month
    while datetime(year,month,1,tzinfo=UTC) <= END:
        result.append(datetime(year,month,1,tzinfo=UTC))
        year,month = (year+1,1) if month==12 else (year,month+1)
    if result[-1] < END:
        result.append(END)  # explicit partial final month, never relabelled a full month
    return result

def period_definitions():
    periods = []
    for year in range(2005,2027):
        periods.append((f'YEAR_{year}', 'calendar', datetime(year,1,1,tzinfo=UTC), min(datetime(year+1,1,1,tzinfo=UTC),END)))
    for a,b in ((2005,2010),(2010,2016),(2016,2022),(2022,2027)):
        periods.append((f'ERA_{a}_{b-1}', 'era', datetime(a,1,1,tzinfo=UTC),min(datetime(b,1,1,tzinfo=UTC),END)))
    for years in (1,2,3,5,10):
        periods.append((f'LATEST_{years}Y', 'recent', END.replace(year=END.year-years), END))
    # Calendar-day boundaries are diagnostic labels, not intraday announcement times.
    periods.extend([
        ('CHF_PRE_FLOOR','policy_diagnostic',START,datetime(2011,9,6,tzinfo=UTC)),
        ('CHF_FLOOR_PERIOD','policy_diagnostic',datetime(2011,9,6,tzinfo=UTC),datetime(2015,1,15,tzinfo=UTC)),
        ('CHF_2015_01_15','policy_diagnostic',datetime(2015,1,15,tzinfo=UTC),datetime(2015,1,16,tzinfo=UTC)),
        ('CHF_AFTER_2015_01_15','policy_diagnostic',datetime(2015,1,16,tzinfo=UTC),END),
    ])
    return periods

def package(work, completed):
    """Atomic publication. An error archive cannot include partial performance."""
    allowed_on_error = {'protocol.md','run_manifest.json','error_report.csv','hard_controls.csv',
                        'software_checks.csv','coverage.csv','data_gaps.csv','runner_source.py',
                        'source_controls.csv','h1_m15_crosschecks.csv','yearly_data_coverage.csv',
                        'coverage_crosscheck_summary.json','archived_pass1_fetch_receipts.csv','parent_pass1_parity.csv','parent_pass1_provenance.json','conditional_feature_controls.csv','archived_anchor_controls.csv','archived_anchor_reference.json'}
    members = [p for p in sorted(work.iterdir()) if p.is_file() and p.name != 'file_manifest.json' and
               (completed or p.name in allowed_on_error)]
    files = [dict(file=p.name,bytes=p.stat().st_size,sha256=file_sha(p)) for p in members]
    write_json(work/'file_manifest.json',files)
    temporary = OUT / (RESULT_NAME+'.tmp')
    with zipfile.ZipFile(temporary,'w',zipfile.ZIP_DEFLATED,compresslevel=6,allowZip64=True) as z:
        for p in members+[work/'file_manifest.json']:
            z.write(p,p.name)
    os.replace(temporary,OUT/RESULT_NAME)

def launch(background=True):
    global STARTED, JOB_LOCK
    with LOCK:
        if STARTED:
            return False
        OUT.mkdir(parents=True,exist_ok=True)
        # Linux Railway/Gunicorn: avoid two workers writing one result archive.
        if os.name == 'posix':
            import fcntl
            handle = (OUT/'run.lock').open('a+')
            try:
                fcntl.flock(handle,fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                STARTED = True
                return False
            JOB_LOCK = handle
        STARTED = True
        # Reuse an exact-version complete result across multiple WSGI workers.
        previous = read_status()
        if previous.get('state')=='complete' and previous.get('runner_sha256')==code_hash() and (OUT/RESULT_NAME).exists():
            if JOB_LOCK is not None:
                JOB_LOCK.close()
                JOB_LOCK = None
            return False
        set_status(state='starting',progress=0,message='Starting EUR/CHF M15 short Pass 3 compression discovery',runner_sha256=code_hash(),result_path=None)
    if background:
        threading.Thread(target=run_job,name='eurchf-m15-pass3-research',daemon=True).start()
        return True
    return run_job()

def app(environ, start_response):
    """Dependency-free WSGI app, including status while the worker is busy."""
    path = environ.get('PATH_INFO','/')
    if path.startswith(PREFIX):
        path = path[len(PREFIX):] or '/'
    method = environ.get('REQUEST_METHOD','GET')
    if method not in ('GET','HEAD'):
        body = b'{"error":"GET or HEAD only; no order endpoints"}'
        start_response('405 Method Not Allowed',[('Content-Type','application/json'),('Content-Length',str(len(body)))])
        return [] if method == 'HEAD' else [body]
    if path not in ('/','/health','/start','/status','/results'):
        body = b'{"error":"Not found"}'
        start_response('404 Not Found',[('Content-Type','application/json'),('Content-Length',str(len(body)))])
        return [] if method == 'HEAD' else [body]
    if path=='/start' or (not STARTED and os.getenv('EURCHF_M15_PASS3_AUTOSTART','1')=='1'):
        launch()
    current = read_status()
    code = '200 OK'
    if path=='/results':
        bundle = OUT/RESULT_NAME
        if current.get('state') in ('complete','error') and current.get('result_path') and bundle.exists():
            start_response(code,[('Content-Type','application/zip'),('Content-Length',str(bundle.stat().st_size)),
                                 ('Content-Disposition',f'attachment; filename="{RESULT_NAME}"'),('Cache-Control','no-store')])
            if method=='HEAD':
                return []
            def chunks():
                with bundle.open('rb') as stream:
                    while True:
                        block = stream.read(1024*1024)
                        if not block:
                            break
                        yield block
            return chunks()
        current = dict(error='Results are not ready',**current)
        code = '409 Conflict'
    elif path=='/health':
        current = dict(ok=True,state=current['state'],version=VERSION,orders_supported=False)
    elif path=='/':
        current = dict(service='EUR/CHF M15 SHORT Pass 3 compression discovery',version=VERSION,status='/status',results='/results',
                       start='/start',research_state=current['state'],orders_supported=False,trading_enabled=False)
    body = (json.dumps(current,allow_nan=False)+'\n').encode()
    start_response(code,[('Content-Type','application/json'),('Content-Length',str(len(body))),('Cache-Control','no-store')])
    return [] if method=='HEAD' else [body]

class ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--run',action='store_true',help='Run the historical study once without an HTTP server')
    group.add_argument('--self-test',action='store_true',help='Synthetic mathematical/execution checks only; no OANDA request')
    args = parser.parse_args()
    if args.self_test:
        print(json.dumps(dict(version=VERSION,market_data_used=False,checks=self_checks()),indent=2))
        return 0
    if args.run:
        return 0 if launch(background=False) else (0 if read_status().get('state')=='complete' else 1)
    port = int(os.getenv('PORT','8080'))
    with make_server('0.0.0.0',port,app,server_class=ThreadedWSGIServer) as server:
        if os.getenv('EURCHF_M15_PASS3_AUTOSTART','1')=='1':
            launch()
        print(f'{VERSION}: listening on {port}; /status and /results',flush=True)
        server.serve_forever()
    return 0

def load_frozen_history(work):
    if not FROZEN_SOURCE_FILE.is_file():
        raise RuntimeError('Missing EURCHF_M15_PASS1_FROZEN_DATA.zip. Upload this file alongside app.py in the same folder.')
    payload=FROZEN_SOURCE_FILE.read_bytes()
    if sha(payload)!=EMBEDDED_PAYLOAD_SHA256:
        raise RuntimeError('Frozen source archive SHA mismatch.')
    shutil.copyfile(FROZEN_SOURCE_FILE,work/'EURCHF_M15_PASS1_FROZEN_DATA.zip')
    with zipfile.ZipFile(io.BytesIO(payload)) as bundle:
        if set(bundle.namelist())!=set(EMBEDDED_MEMBER_PINS) or bundle.testzip() is not None:
            raise RuntimeError('Frozen source archive members/CRC mismatch.')
        for name,pin in EMBEDDED_MEMBER_PINS.items():
            data=bundle.read(name)
            if len(data)!=pin['bytes'] or sha(data)!=pin['sha256']:
                raise RuntimeError('Frozen source member SHA/size mismatch: '+name)
            destination='archived_pass1_fetch_receipts.csv' if name=='fetch_receipts.csv' else name
            if name in ('source_candles.csv','source_h1_crosscheck_candles.csv','fetch_receipts.csv'):
                (work/destination).write_bytes(data)
    def read_source(name,step):
        bars,volumes=[],{}
        with (work/name).open(newline='') as stream:
            for row in csv.DictReader(stream):
                t=when(row['time']);v=int(row['volume'])
                if v<0:raise RuntimeError('Negative archived price count.')
                bars.append((t,*[float(row[k]) for k in ('open','high','low','close')]))
                volumes[t]=v
        validate_bars(bars,step)
        return bars,volumes
    bars,volumes=read_source('source_candles.csv',BAR)
    h1,hvolumes=read_source('source_h1_crosscheck_candles.csv',H1_BAR)
    if len(bars)!=546647 or source_hash(bars)!='63eeea194c7106cfe0ebe248e96a392ab876611da5fc2fb569e3b2f304eaaa22':
        raise RuntimeError('Pinned Pass 1 M15 source mismatch.')
    if len(h1)!=137939 or source_hash(h1)!='b3b686ad3a92569d971d2d9412c8afe64a5ad8f54b5d097840c4a2d6cebbd2c0':
        raise RuntimeError('Pinned Pass 1 H1 source mismatch.')
    return bars,volumes,h1,hvolumes

BOUNDS=month_bounds()
PERIODS=period_definitions()


PROTOCOL='''# EURCHF M15 SHORT — Pass 3 compression/downside-breakout discovery

Predeclared 8 October 2026 before calculating this mechanism's historical returns.
The engulfing and M15-volatility branches remain parked; their failed tests remain
in the search record. This is a standalone new mechanism, not a combined portfolio.
Full previously inspected history remains exploratory/in-sample.

## Fixed entry definition
Use the SAME frozen native OANDA MID M15 companion ZIP, requested 2005-01-01
through 2026-10-08T00Z exclusive. Common warmup200. No new API call or variable.
Prior box uses N observed candles [i-N,i), excluding the current signal candle.
boxHigh=max(highs); boxLow=min(lows); require positive width and positive prior ATR.
Compression = (boxHigh-boxLow)/ATR14[i-1]. ATR is TR1..14 mean then Wilder.
Current completed signal must be bearish (close<open), with close STRICTLY below
boxLow. A wick below, an equal close, a bullish candle or a doji is not a trigger.
Breakout strength = (open-close)/ATR14[i-1], with inclusive minimum threshold.
Entry reference is signal close, decision/entry time = signal open+15 minutes.
No engulfing requirement, signal-high structure-distance rule, daily context,
momentum, signal-close-location, time/day exclusions, retest or pending order.

## Bounded grid and controls
27 research settings: N8/12/16 observed bars (normally2/3/4hours), compression
maximum1.5/2.0/2.5 prior ATR, bearish-body minimum0/.25/.50 prior ATR, full3x3x3.
Three breakdown-only references: each N, no compression maximum, body minimum0.
Two exact archived engulf anchors: MAIN L200/D.10/body1.25 and TIGHT D.075.
These anchors are integrity/context controls, not competitors eligible for new
selection. They retain their original stop/target/eligibility and actual returned
Pass2C complete accepted-ledger hashes. All12 archive cases must pass first.
Total32 unique definitions/192 cases:27research/162cases,3mechanismreferences/18,
2archiveanchors/12. Neighbours change exactly one grid coordinate by one step;
controls never enter the new-grid plateau summaries. No automatic winner.

## New mechanism stop, fixed RR and execution assumptions
Compression trades: stop=max(prior boxHigh, signalHigh)+0.00010 (one pip).
This represents invalidation above the range and breakout candle; it was specified
before this mechanism's returns, not selected from successful stop-size buckets.
Archive anchors: signalHigh+0.00010 unchanged. Target=referenceClose-3*(stop-close).
RR3 is fixed, not optimized. Assumed adverse SELL entry costs10/20/40ticks=1/2/4pips;
fill=signalClose-cost*TICK. Keep stop/target fixed and denominator=stop-fill.
Require0<target<fill<stop; invalid entries do not occupy a position. No stop-size
filter or cost relaxation. STOP normal loss=-1R; opening gaps can be worse.

Full chronological one-position replay separately per definition/model/cost;
signal on exit candle may enter at its close, unresolved positions occupy to
cutoff and have no invented R. Never filter an already accepted control ledger.
Primary STOP_FIRST_GAP_STRESS: adverse opening gaps max(open,stop), favourable
target gaps capped, intrabar dual touch loses. Opening-gap exit timed at candle
open; ordinary exit at close. Alternate NEAREST_OPEN_SENSITIVITY retains archived
nearest-extreme/tie-loss/barrier-fill behavior; it is a sensitivity assumption.
Price paths are distinct for each box length and archived stop definition.
Cross-geometry comparisons explicitly retain different R for common signals.

## Data and evidence
Exact companion ZIP/member CRC/byte/hash gates; pinned546647 M15 and137939 H1;
H1 OHLC/price-count aggregation and annual coverage controls unchanged.
Windows count observed candles across closures/sparse history; do not invent
bars or treat N as guaranteed elapsed hours. Export box start/end/span and closure
diagnostics. No newly introduced missing-bar or session exclusion.
Exports: complete source/ATR/raw-break features, grid/roles/membership, signal
paths per geometry, full accepted ledgers, invalid/open rows, all annual/era and
latest1/2/3/5/10-year periods, monthly zeros/droughts/blank years, complete rolling
12/24/36-month windows including zeros; partial2026/October marked. Entry cohort
[start,end) and realized cash(start,end] are distinct. IncompleteOctober is not
a rolling endpoint. CHF shock exposure is attribution, not an exclusion.
Matched breakdown-reference comparisons and full-replay added/removed entries;
raw/accepted signal equivalence and exact execution-equivalence groups; one-axis
neighbourhoods and fixed descriptive stop/cost-risk buckets. All controls kept.
Common-entry R can differ with stop geometry; delta attribution includes that
component. Accepted index overlap alone does not mean identical trades.

## Decision budget
Look for an interpretable neighbourhood, useful trade count, acceptable costs and
temporal/rolling support. Best single cell, tiny sample, duplicate streams, one
winner or smaller drawdown alone is insufficient. No adaptive grid expansion,
filter stacking, exit/RR tuning or combined engulf replay in this pass. Review
the results before deciding whether a bounded confirmation is justified; stop
this mechanism if evidence is weak. RR last only after entry evidence earns a
freeze. Exact portfolio comparison, independent evidence and prospective
execution remain later stages. MID plus assumed entry costs omits actual bid/ask,
ask-side short stops, financing and executable fills. R and closed DD are not NAV%.
No broker credentials, network fetch, order function or live-service modification.
'''
RESULT_README='''# EURCHF M15 SHORT Pass3 results
Check complete=true and every software/source/hard/archive gate PASS first.
32definitions/192cases:27compression settings,3breakdown-only controls,2archives.
Read protocol.md before judging any setting. Same full native M15 source and
RR3/1,2,4pip assumptions. New stop=max(boxHigh,signalHigh)+1pip; archived stops
unchanged. Neighbourhoods cover only the27 new research definitions.
matched_breakdown_comparisons.csv and marginal_entry_differences.csv use full
replay, not filtered accepted trades. accepted_comparisons.csv also compares
archived anchors, allowing different common-entry R across stop definitions.
equivalence_groups.csv distinguishes same signals from same execution geometry.
All yearly/monthly/rolling zeros are retained. No automatic winner/live admission.
Return this complete ZIP for review. All history remains exploratory/in-sample.
'''

def make_configs():
    configs=[]
    for cid,distance in zip(PARENT_IDS,(.10,.075)):
        configs.append(dict(config_id=cid,role='ARCHIVED_ANCHOR',geometry_key='ARCHIVE',
            box_bars=200,width_max_atr=None,body_min_atr=1.25,distance_atr=distance))
    for n in BOX_WINDOWS:
        configs.append(dict(config_id=f'BREAKDOWN_ONLY_N{n:02d}',role='MECHANISM_REFERENCE',
            geometry_key=f'BOX_N{n:02d}',box_bars=n,width_max_atr=None,body_min_atr=0.,distance_atr=None))
        for width in WIDTH_CAPS:
            for body in BODY_FLOORS:
                cid=f'COMP_N{n:02d}_W{round(width*100):03d}_B{round(body*100):03d}'
                configs.append(dict(config_id=cid,role='RESEARCH',geometry_key=f'BOX_N{n:02d}',
                    box_bars=n,width_max_atr=width,body_min_atr=body,distance_atr=None))
    membership=[dict(config_id=c['config_id'],role=c['role']) for c in configs]
    return configs,membership

def make_features(bars):
    atr=atr14(bars)
    highs=[b[2] for b in bars];lows=[-b[3] for b in bars]
    previous={n:(previous_high(highs,n),previous_high(lows,n)) for n in BOX_WINDOWS}
    archive_high=previous_high(highs,200)
    features={g:{} for g in ['ARCHIVE']+[f'BOX_N{n:02d}' for n in BOX_WINDOWS]}
    for i in range(WARMUP,len(bars)):
        t,op,hi,lo,cl=bars[i]
        if atr[i] is not None and atr[i]>0 and bearish_engulf(bars,i):
            features['ARCHIVE'][i]=dict(signal_index=i,signal=iso(t),entry=iso(t+BAR),
                atr14=atr[i],body_atr=abs(cl-op)/atr[i],
                abs_distance_atr_200=abs(hi-archive_high[i])/atr[i],previous_high_200=archive_high[i])
        if not cl<op or atr[i-1] is None or atr[i-1]<=0:continue
        for n,(hh,ll) in previous.items():
            box_high,box_low=hh[i],-ll[i]
            if not box_high>box_low or not cl<box_low:continue
            features[f'BOX_N{n:02d}'][i]=dict(signal_index=i,signal=iso(t),entry=iso(t+BAR),
                box_bars=n,box_start=iso(bars[i-n][0]),box_end=iso(t),
                box_span_hours=(t-bars[i-n][0]).total_seconds()/3600,
                box_has_missing_intervals=int(t-bars[i-n][0]!=n*BAR),box_high=box_high,box_low=box_low,
                prior_atr14=atr[i-1],width_atr=(box_high-box_low)/atr[i-1],
                bearish_body_atr=(op-cl)/atr[i-1],open=op,high=hi,low=lo,close=cl,
                stop=max(box_high,hi)+10*TICK,
                reference_stop_pips=(max(box_high,hi)+10*TICK-cl)/PIP)
    return features

def selected_indices(config,features):
    universe=features[config['geometry_key']]
    if config['role']=='ARCHIVED_ANCHOR':
        return [i for i,r in universe.items() if r['body_atr']>=config['body_min_atr']
            and r['abs_distance_atr_200']<=config['distance_atr']]
    return [i for i,r in universe.items() if r['bearish_body_atr']>=config['body_min_atr']
        and (config['width_max_atr'] is None or r['width_atr']<=config['width_max_atr'])]

def build_paths(bars,features,configs,archive_only=False):
    wanted=defaultdict(set)
    for config in configs:
        if archive_only and config['role']!='ARCHIVED_ANCHOR':continue
        wanted[config['geometry_key']].update(selected_indices(config,features))
    result={}
    for geo,ids in wanted.items():
        result[geo]={i:find_paths(bars,i,None if geo=='ARCHIVE' else features[geo][i]['stop']) for i in sorted(ids)}
    return result

def hard_controls(bars,features,configs):
    checks=[]
    def check(name,condition):
        checks.append(dict(check=name,status='PASS' if condition else 'FAIL'))
    check('32_unique_configs_27_research',len(configs)==32 and len({c['config_id'] for c in configs})==32
        and sum(c['role']=='RESEARCH' for c in configs)==27)
    tr=[max(bars[i][2]-bars[i][3],abs(bars[i][2]-bars[i-1][4]),abs(bars[i][3]-bars[i-1][4])) for i in range(1,len(bars))]
    reference=[None]*len(bars)
    if len(bars)>14:
        reference[14]=math.fsum(tr[:14])/14
        for i in range(15,len(bars)):reference[i]=reference[i-1]+(tr[i-1]-reference[i-1])/14
    actual=atr14(bars)
    check('independent_full_ATR_seed_recurrence',all((a is None and b is None) or (a is not None and b is not None and abs(a-b)<=1e-12) for a,b in zip(actual,reference)))
    for n in BOX_WINDOWS:
        geo=f'BOX_N{n:02d}';expected=[];max_error=0.
        for i in range(WARMUP,len(bars)):
            prior=bars[i-n:i];hi=max(b[2] for b in prior);lo=min(b[3] for b in prior)
            t,op,signal_high,signal_low,cl=bars[i]
            if cl<op and cl<lo and hi>lo and reference[i-1] is not None and reference[i-1]>0:
                expected.append(i);row=features[geo].get(i)
                if row is None:continue
                max_error=max(max_error,abs(row['width_atr']-(hi-lo)/reference[i-1]),
                    abs(row['bearish_body_atr']-(op-cl)/reference[i-1]),abs(row['stop']-(max(hi,signal_high)+10*TICK)))
        check(geo+'_all_strict_break_indices_direct_slices',list(features[geo])==expected)
        check(geo+'_features_exclude_current_bar',max_error<=1e-10)
    expected_archive=[i for i in range(WARMUP,len(bars)) if bars[i-1][4]>bars[i-1][1]
        and bars[i][4]<bars[i][1] and bars[i][1]>=bars[i-1][4] and bars[i][4]<=bars[i-1][1]
        and reference[i] is not None and reference[i]>0]
    check('archive_raw_engulf_matches_independent_predicate',list(features['ARCHIVE'])==expected_archive)
    for c in configs:
        expected=[]
        for i,r in features[c['geometry_key']].items():
            if c['role']=='ARCHIVED_ANCHOR':
                hi=max(b[2] for b in bars[i-200:i]);eligible=(bars[i][1]-bars[i][4])/reference[i]>=1.25 and abs(bars[i][2]-hi)/reference[i]<=c['distance_atr']
            else:
                eligible=r['bearish_body_atr']>=c['body_min_atr'] and (c['width_max_atr'] is None or r['width_atr']<=c['width_max_atr'])
            if eligible:expected.append(i)
        check(c['config_id']+'_independent_qualification_mask',selected_indices(c,features)==expected)
    # Prefix causality checks feature values, not subsequent exits/performance.
    limit=min(10000,len(bars));prefix=make_features(bars[:limit])
    check('feature_prefix_causality_all_geometries',all(prefix[geo]=={i:r for i,r in rows.items() if i<limit} for geo,rows in features.items()))
    return checks

def archived_controls(work,bars,features,path_sets,configs):
    payload=zlib.decompress(base64.b85decode(ARCHIVE_REFERENCE_B85.encode()))
    if sha(payload)!=ARCHIVE_REFERENCE_SHA256:raise RuntimeError('Archived reference checksum mismatch')
    reference=json.loads(payload)
    if len(reference['rows'])!=1080 or len(reference['summaries'])!=12:raise RuntimeError('Incomplete archived anchors')
    old={(r['config_id'],r['execution_model'],int(r['cost_ticks'])):r for r in reference['summaries']}
    wanted=defaultdict(list)
    for r in reference['rows']:wanted[r['config_id'],r['execution_model'],int(r['cost_ticks'])].append(r)
    checks=[];numeric={'reference_entry','historical_fill','stop','target','risk_price','exit_price','r'}
    integer={'cost_ticks','accepted_sequence','signal_index','exit_index'}
    for c in configs:
        if c['role']!='ARCHIVED_ANCHOR':continue
        paths=path_sets['ARCHIVE'];ids=selected_indices(c,features)
        for model in MODELS:
            for cost in COSTS:
                key=(c['config_id'],model,cost);digest=hashlib.sha256();accepted,invalid,blocked=replay(ids,paths,model,cost)
                ledger=[]
                for seq,i in enumerate(accepted,1):
                    p=paths[i][model];fill=geometry(p,cost)[0]
                    full=dict(config_id=c['config_id'],execution_model=model,cost_ticks=cost,accepted_sequence=seq,
                        signal_index=i,exit_index=p['exit_index'],signal=p['signal'],entry=p['entry'],exit=p['exit'],
                        reference_entry=p['reference_entry'],historical_fill=fill,stop=p['stop'],target=p['target'],
                        risk_price=p['stop']-fill,exit_price=p['exit_price'],reason=p['reason'],r=r_value(p,cost))
                    ledger.append(full);digest.update((json.dumps(full,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode())
                archived=wanted[key];okay=len(ledger)==len(archived)
                for actual,expected in zip(ledger,archived):
                    for field,value in actual.items():
                        if value is None:okay=okay and expected[field]==''
                        elif field in numeric:okay=okay and abs(float(expected[field])-value)<=1e-10
                        elif field in integer:okay=okay and int(expected[field])==value
                        else:okay=okay and str(value)==expected[field]
                okay=okay and digest.hexdigest()==old[key]['accepted_ledger_sha256'] and signal_hash(ids,bars)==old[key]['raw_signal_sha256']
                okay=okay and blocked==int(old[key]['p0_blocked_signals']) and len(invalid)==int(old[key]['invalid_unblocked_entries'])
                for field,value in stats(r['r'] for r in ledger if r['r'] is not None).items():
                    expected=old[key][field]
                    okay=okay and ((value is None and expected=='') or (value is not None and expected!='' and abs(value-float(expected))<=1e-9))
                checks.append(dict(config_id=key[0],execution_model=model,cost_ticks=cost,status='PASS' if okay else 'FAIL',
                    check='actual_returned_complete_anchor_ledger_hash_fields_signals_metrics',accepted_rows=len(accepted)))
    write_csv(work/'archived_anchor_controls.csv',checks)
    write_json(work/'archived_anchor_reference.json',reference)
    if any(r['status']!='PASS' for r in checks):raise RuntimeError('Archived anchor parity failed before new outcomes')
    return checks

def write_inputs(work,bars,features,path_sets,configs,memberships,dataset_kind,volumes=None):
    write_csv(work/'configuration_grid.csv',configs)
    write_csv(work/'research_case_roles.csv',memberships)
    write_csv(work/'period_definitions.csv',(dict(period=label,period_type=kind,start=iso(a),end=iso(b)) for label,kind,a,b in PERIODS))
    write_csv(work/'coverage.csv',[dict(dataset_kind=dataset_kind,source_candles=len(bars),first=iso(bars[0][0]),
        last=iso(bars[-1][0]),requested_start=iso(START),requested_end=iso(END),source_sha256=source_hash(bars))])
    write_csv(work/'data_gaps.csv',(dict(previous=iso(a[0]),next=iso(b[0]),elapsed_minutes=(b[0]-a[0]).total_seconds()/60,
        absent_intervals=round((b[0]-a[0]).total_seconds()/900)-1,interpretation='Observed gap; no synthetic bars or automatic exclusion')
        for a,b in zip(bars,bars[1:]) if b[0]-a[0]!=BAR),['previous','next','elapsed_minutes','absent_intervals','interpretation'])
    write_csv(work/'source_atr14.csv',(dict(signal_index=i,time=iso(bars[i][0]),atr14=atr) for i,atr in enumerate(atr14(bars))))
    fields=['geometry_key','signal_index','signal','entry','box_bars','box_start','box_end','box_span_hours',
        'box_has_missing_intervals','box_high','box_low','prior_atr14','width_atr','bearish_body_atr','open','high','low','close','stop','reference_stop_pips']
    write_csv(work/'raw_breakdown_features.csv',(dict(geometry_key=geo,**r) for geo,rows in features.items() if geo!='ARCHIVE' for r in rows.values()),fields)
    write_csv(work/'archived_engulf_features.csv',features['ARCHIVE'].values(),['signal_index','signal','entry','atr14','body_atr','abs_distance_atr_200','previous_high_200'])
    fields=['geometry_key','execution_model','signal_index','signal','entry','reference_entry','stop','target','next_open',
        'next_candle_delay_hours','exit_index','exit','exit_price','reason','dual_touch','gap_stop','gap_target']
    write_csv(work/'signal_trade_paths.csv',(dict(geometry_key=geo,execution_model=model,**p) for geo,paths in path_sets.items() for i,models in paths.items() for model,p in models.items()),fields)

def write_neighbours(work,configs,summaries):
    research=[c for c in configs if c['role']=='RESEARCH'];rows=[]
    grid={(c['box_bars'],c['width_max_atr'],c['body_min_atr']):c for c in research}
    for c in research:
        neighbours=[];edges=[]
        coordinates=[c['box_bars'],c['width_max_atr'],c['body_min_atr']]
        for axis,values in enumerate((BOX_WINDOWS,WIDTH_CAPS,BODY_FLOORS)):
            pos=values.index(coordinates[axis])
            if pos in (0,len(values)-1):edges.append(('box_bars','width_max_atr','body_min_atr')[axis]+('='+'LOWER' if pos==0 else '=UPPER'))
            for step in (-1,1):
                q=pos+step
                if 0<=q<len(values):
                    candidate=coordinates.copy();candidate[axis]=values[q];neighbours.append(grid[tuple(candidate)]['config_id'])
        for model in MODELS:
            for cost in COSTS:
                ours=summaries[c['config_id'],model,cost];other=[summaries[cid,model,cost] for cid in neighbours]
                rows.append(dict(config_id=c['config_id'],execution_model=model,cost_ticks=cost,tested_boundaries=';'.join(edges),
                    adjacent_configurations=len(other),distinct_neighbour_raw_streams=len({r['raw_signal_sha256'] for r in other}),
                    neighbours_identical_to_this_raw_stream=sum(r['raw_signal_sha256']==ours['raw_signal_sha256'] for r in other),
                    positive_total_r_neighbours=sum(r['total_r']>0 for r in other),
                    distinct_positive_neighbour_raw_streams=len({r['raw_signal_sha256'] for r in other if r['total_r']>0}),
                    median_neighbour_total_r=quantile([r['total_r'] for r in other],.5),
                    minimum_neighbour_total_r=min(r['total_r'] for r in other),
                    minimum_neighbour_closed_trades=min(r['closed_trades'] for r in other),neighbour_ids=';'.join(sorted(neighbours))))
    write_csv(work/'neighbourhood_summary.csv',rows)

def export_matched_comparisons(work,configs):
    byid={c['config_id']:c for c in configs};rows=[]
    with (work/'accepted_comparisons.csv').open(newline='') as f:
        for row in csv.DictReader(f):
            c=byid[row['config_id']]
            if c['role']=='RESEARCH' and row['reference_config']==f"BREAKDOWN_ONLY_N{c['box_bars']:02d}":rows.append(row)
    fields=CASE+['reference_config','common_entries','added_entries','removed_entries','common_completed_r_candidate',
        'common_completed_r_reference','added_completed_r','removed_completed_r','candidate_total_r','reference_total_r',
        'delta_total_r','candidate_open_count','reference_open_count']
    write_csv(work/'matched_breakdown_comparisons.csv',rows,fields)

def export_extra_diagnostics(work,bars,features,path_sets,configs):
    ledgers=defaultdict(list);configmap={c['config_id']:c for c in configs};groups=defaultdict(list)
    with (work/'accepted_ledgers.csv').open(newline='') as f:
        for row in csv.DictReader(f):ledgers[row['config_id'],row['execution_model'],int(row['cost_ticks'])].append(row)
    marginal=[];buckets=[]
    for c in configs:
        raw=selected_indices(c,features)
        groups['RAW','ALL',0,signal_hash(raw,bars)].append(c['config_id'])
        for model in MODELS:
            for cost in COSTS:
                key=(c['config_id'],model,cost);ledger=ledgers[key]
                stream=sha('\n'.join(r['signal_index'] for r in ledger).encode())
                executed=sha('\n'.join(json.dumps({k:v for k,v in r.items() if k!='config_id'},sort_keys=True,separators=(',',':')) for r in ledger).encode())
                groups['ACCEPTED_SIGNAL',model,cost,stream].append(c['config_id'])
                groups['ACCEPTED_EXECUTION',model,cost,executed].append(c['config_id'])
                if c['role']=='RESEARCH':
                    reference=f"BREAKDOWN_ONLY_N{c['box_bars']:02d}";other=ledgers[reference,model,cost]
                    ours={int(r['signal_index']):r for r in ledger};theirs={int(r['signal_index']):r for r in other}
                    rawset=set(raw);refset=set(selected_indices(configmap[reference],features))
                    for label,indices,source in [('ADDED',ours.keys()-theirs.keys(),ours),('REMOVED',theirs.keys()-ours.keys(),theirs)]:
                        for i in sorted(indices):
                            r=source[i];marginal.append(dict(config_id=c['config_id'],execution_model=model,cost_ticks=cost,
                                reference_config=reference,change=label,signal_index=i,signal=r['signal'],entry=r['entry'],exit=r['exit'],
                                reason=r['reason'],r=r['r'],qualifies_in_candidate=int(i in rawset),qualifies_in_reference=int(i in refset),
                                interpretation='Full separate replay; replacements/displacements may occur'))
                for axis,limits,labels in [('stop_pips',(5.,10.,20.,40.),('<=5','(5,10]','(10,20]','(20,40]','>40')),
                    ('cost_fraction_reference_risk',(.05,.10,.20,.40),('<=.05','(.05,.10]','(.10,.20]','(.20,.40]','>.40'))]:
                    assigned=[[] for _ in labels]
                    for r in ledger:
                        risk=float(r['stop'])-float(r['reference_entry'])
                        value=risk/PIP if axis=='stop_pips' else cost*TICK/risk
                        assigned[bisect.bisect_left(limits,value)].append(r)
                    for label,subset in zip(labels,assigned):
                        rs=[float(r['r']) for r in subset if r['r']!='']
                        buckets.append(dict(config_id=c['config_id'],execution_model=model,cost_ticks=cost,bucket_axis=axis,bucket=label,
                            accepted_entries=len(subset),closed_trades=len(rs),open_trades=len(subset)-len(rs),total_r=math.fsum(rs),
                            winners=sum(r>0 for r in rs),losers=sum(r<0 for r in rs),interpretation='Fixed descriptive attribution, not optimized stop filter'))
    fields=CASE+['reference_config','change','signal_index','signal','entry','exit','reason','r','qualifies_in_candidate','qualifies_in_reference','interpretation']
    counts={}
    counts['marginal_entry_differences.csv']=write_csv(work/'marginal_entry_differences.csv',marginal,fields)
    counts['stop_cost_attribution.csv']=write_csv(work/'stop_cost_attribution.csv',buckets)
    equivalence=[dict(equivalence_kind=kind,execution_model=model,cost_ticks=cost,fingerprint=digest,configuration_count=len(ids),
        config_ids=';'.join(sorted(ids)),research_configuration_count=sum(configmap[i]['role']=='RESEARCH' for i in ids),
        interpretation='Same signals do not imply same stop/R; shared streams are not independent evidence') for (kind,model,cost,digest),ids in sorted(groups.items())]
    counts['equivalence_groups.csv']=write_csv(work/'equivalence_groups.csv',equivalence)
    return counts

def self_checks():
    checks=[]
    def check(ok,name):
        if not ok:raise AssertionError(name)
        checks.append(dict(check=name,status='PASS',evidence='SYNTHETIC_SOFTWARE_ONLY'))
    c,roles=make_configs()
    check(len(c)==32 and len(roles)==32 and sum(r['role']=='RESEARCH' for r in c)==27,'Complete bounded27/3/2 grid')
    check(len(BOUNDS)==263 and len(PERIODS)==35,'Full zero-inclusive period/month universe')
    t=datetime(2020,1,1,tzinfo=UTC)
    bars=[(t+i*BAR,1.,1.0005,.9995,1.) for i in range(202)]
    bars[200]=(t+200*BAR,1.,1.0007,.999,.99925)
    f=make_features(bars)
    check(all(200 in f[f'BOX_N{n:02d}'] for n in BOX_WINDOWS),'Strict bearish close below prior box triggers')
    check(all(abs(f[f'BOX_N{n:02d}'][200]['box_high']-1.0005)<1e-12 for n in BOX_WINDOWS),'Signal high excluded from prior range')
    check(abs(f['BOX_N08'][200]['stop']-1.0008)<1e-12,'Stop includes breakout overshoot and one pip')
    equal=bars.copy();equal[200]=(t+200*BAR,1.,1.0005,.999,.9995)
    check(all(200 not in r for geo,r in make_features(equal).items() if geo!='ARCHIVE'),'Close equality at box low rejected')
    bullish=bars.copy();bullish[200]=(t+200*BAR,.999,.9995,.9985,.99925)
    check(all(200 not in r for geo,r in make_features(bullish).items() if geo!='ARCHIVE'),'Bullish below-box candle rejected')
    previous=previous_high([1.,3.,2.,100.,4.,5.],3)
    check(previous==[None,None,None,3.,100.,100.],'Prior monotonic queue excludes current high')
    row=dict(f['BOX_N08'][200]);row['width_atr']=2.;row['bearish_body_atr']=.25
    fixture_features={'BOX_N08':{200:row}}
    config=next(x for x in c if x['config_id']=='COMP_N08_W200_B025')
    check(selected_indices(config,fixture_features)==[200],'Compression/body threshold equalities included')
    row['width_atr']=2.00001
    check(selected_indices(config,fixture_features)==[],'Compression above cap rejected')
    dual=[(t,10.5,11.,9.,10.),(t+BAR,7.2,11.2,6.8,8.)]
    p=find_paths(dual,0)
    check(p[MODELS[0]]['reason']=='TARGET' and p[MODELS[1]]['reason']=='STOP','Dual-touch sensitivity and stress differ')
    widened=find_paths(dual,0,12.)
    check(widened[MODELS[0]]['reason']=='OPEN_AT_DATA_END','Box-specific stop actually changes path')
    gap=find_paths([(t,10.5,11.,9.,10.),(t+BAR,12.,12.1,11.5,11.8)],0)[MODELS[1]]
    check(gap['exit_price']==12. and r_value(gap,10)<-1 and gap['exit']==iso(t+BAR),'Adverse opening gap R and timestamp')
    target=find_paths([(t,10.5,11.,9.,10.),(t+BAR,6.,6.5,5.5,6.2)],0)[MODELS[1]]
    check(target['exit_price']==target['target'] and target['gap_target']==1,'Target opening gap capped')
    fixture=dict(signal_index=0,signal=iso(t),entry=iso(t+BAR),reference_entry=1.,stop=1.001,target=.997,
        exit_index=None,exit=None,exit_price=None,reason='OPEN_AT_DATA_END',dual_touch=0,gap_stop=0,gap_target=0)
    mapping={i:{model:dict(fixture,signal_index=i) for model in MODELS} for i in (0,1,2)}
    check(replay([0,1,2],mapping,MODELS[0],10)[0]==[0],'Open trade occupies position to end')
    mapping[0][MODELS[0]].update(exit_index=1,exit=iso(t+2*BAR),exit_price=1.001,reason='STOP')
    check(replay([0,1,2],mapping,MODELS[0],10)[0]==[0,1],'Exit-candle signal can reenter')
    mapping[0][MODELS[0]]['target']=.9998
    check(replay([0,1,2],mapping,MODELS[0],40)[0]==[1],'Invalid geometry does not occupy position')
    check(geometry(dict(fixture,entry=iso(END)),10)[1]=='ENTRY_AT_OR_AFTER_CUTOFF','Exclusive cutoff rejects new entry')
    check(r_value(fixture,10) is None,'Unresolved trade has no invented R')
    check(stats([])['total_r']==0 and stats([2.,-1.,-1.,-1.,2.])['max_closed_dd_r']==-3.,'Empty results and chronological DD')
    return checks

def run_job():
    global RUN_CLOCK,JOB_LOCK
    RUN_CLOCK=time.monotonic();OUT.mkdir(parents=True,exist_ok=True)
    work=Path(tempfile.mkdtemp(prefix='working-',dir=OUT))
    manifest=dict(version=VERSION,runner_sha256=code_hash(),pair=PAIR,side=SIDE,timeframe=TIMEFRAME,
        complete=False,status='RUNNING',study='PASS3_BOUNDED_COMPRESSION_DISCOVERY',dataset_kind='COMPANION_ZIP_EXACT_PASS1_OANDA_MID',
        frozen_source_archive_sha256=EMBEDDED_PAYLOAD_SHA256,start=iso(START),end_exclusive=iso(END),rr=RR,
        cost_ticks=list(COSTS),execution_models=list(MODELS),expected_configurations=32,expected_cases=192,
        new_configurations=27,new_cases=162,mechanism_references=3,archived_anchors=2,
        compression_stop='MAX_PRIOR_BOX_HIGH_SIGNAL_HIGH_PLUS_1_PIP',anchor_stop='UNCHANGED_SIGNAL_HIGH_PLUS_1_PIP',
        research_grid=dict(box_bars=BOX_WINDOWS,width_max_atr=WIDTH_CAPS,body_min_atr=BODY_FLOORS),
        portfolio_target='PORTFOLIO32_2026_10_05_EURCHF_H1_SHORT_PRIMARY_RR3P50_V1; admission deferred',
        orders_supported=False,trading_enabled=False)
    try:
        (work/'protocol.md').write_text(PROTOCOL);shutil.copyfile(__file__,work/'runner_source.py')
        write_csv(work/'software_checks.csv',self_checks())
        set_status(state='validating',progress=2,message='Checking exact companion source and full-history coverage')
        bars,volumes,h1,hvolumes=load_frozen_history(work)
        crosscheck_history(work,bars,volumes,h1,hvolumes)
        configs,memberships=make_configs();features=make_features(bars)
        controls=hard_controls(bars,features,configs);write_csv(work/'hard_controls.csv',controls)
        if any(r['status']!='PASS' for r in controls):raise RuntimeError('Independent signal/feature controls failed')
        archive_paths=build_paths(bars,features,configs,archive_only=True)
        archived_controls(work,bars,features,archive_paths,configs)
        set_status(state='building_paths',progress=30,message='All source/feature/archived gates passed; building box-specific paths')
        paths=build_paths(bars,features,configs)
        write_inputs(work,bars,features,paths,configs,memberships,manifest['dataset_kind'],volumes)
        (work/'README.md').write_text(RESULT_README)
        _,counts=analyze(work,bars,features,paths,configs)
        manifest.update(complete=True,status='COMPLETE',source_sha256=source_hash(bars),source_candles=len(bars),
            source_h1_sha256=source_hash(h1),source_controls='PASS',hard_controls='PASS',archived_anchor_controls='PASS',
            software_controls='PASS',protocol_sha256=sha(PROTOCOL.encode()),output_row_counts=counts,
            completed_at=iso(datetime.now(UTC)),elapsed_seconds=round(time.monotonic()-RUN_CLOCK,1))
        write_json(work/'run_manifest.json',manifest)
        set_status(state='packaging',progress=96,message='All192 cases complete; packaging reviewable outputs')
        package(work,True)
        set_status(state='complete',progress=100,message='EURCHF M15 SHORT Pass3 complete; download results ZIP',
            configurations=32,cases=192,result_path='/results',result_bytes=(OUT/RESULT_NAME).stat().st_size)
        return True
    except Exception as exc:
        manifest.update(status='ERROR',complete=False,error_type=type(exc).__name__,error=str(exc))
        write_json(work/'run_manifest.json',manifest)
        write_csv(work/'error_report.csv',[dict(error_type=type(exc).__name__,message=str(exc),traceback=traceback.format_exc())])
        try:package(work,False);result_path='/results'
        except Exception:result_path=None
        set_status(state='error',progress=100,message=str(exc),result_path=result_path)
        return False
    finally:
        shutil.rmtree(work,ignore_errors=True)
        if JOB_LOCK is not None:JOB_LOCK.close();JOB_LOCK=None


def analyze(work, bars, features, path_sets, configs, *, progress=True):
    """Pure reporting/replay layer. Production calls this only after hard gates."""
    sinks = {}
    definitions = {
        'summary.csv': SUMMARY_FIELDS,
        'raw_signal_membership.csv': ['config_id','signal_index'],
        'accepted_trades.csv': CASE+['accepted_sequence','signal_index','historical_fill','risk_price','r'],
        'invalid_entries.csv': CASE+['signal_index','reason'],
        'open_at_data_end.csv': CASE+['signal_index','entry','historical_fill','stop','target','held_hours_to_cutoff'],
        'control_accepted_ledgers.csv': CASE+['accepted_sequence','signal_index','exit_index','signal','entry','exit',
             'reference_entry','historical_fill','stop','target','risk_price','exit_price','reason','r'],
        'accepted_ledgers.csv': CASE+['accepted_sequence','signal_index','exit_index','signal','entry','exit',
             'reference_entry','historical_fill','stop','target','risk_price','exit_price','reason','r'],
        'period_results.csv': CASE+['period','period_type','start','end','partial_calendar_year','entry_count',
             'entry_cohort_completed','entry_cohort_eventual_r','entry_cohort_open',
             'exit_count','realized_r','realized_max_dd_r','open_at_period_end'],
        'monthly_results.csv': CASE+['month','start','end','partial_month','entry_count','exit_count','realized_r','entry_cohort_eventual_r','entry_cohort_open'],
        'rolling_windows.csv': CASE+['months','start','end','entry_count','exit_count','realized_r','zero_entry_window','zero_exit_window'],
        'accepted_comparisons.csv': CASE+['reference_config','common_entries','added_entries','removed_entries',
             'common_completed_r_candidate','common_completed_r_reference','added_completed_r','removed_completed_r',
             'candidate_total_r','reference_total_r','delta_total_r','candidate_open_count','reference_open_count'],
        'chf_event_exposure.csv': CASE+['signal_index','entry','exit','entered_on_event_day','exited_on_event_day',
             'held_across_event_day_start','eventual_r','outcome','attribution_warning'],
    }
    for name,fields in definitions.items():
        sinks[name] = CSVFile(work/name,fields)
    summaries = {}
    times_cache={geo:{(i,model):(when(p['entry']),when(p['exit']) if p['exit'] else None)
        for i,models in paths.items() for model,p in models.items()} for geo,paths in path_sets.items()}
    outcome_cache={geo:{(model,cost):{i:r_value(paths[i][model],cost) for i in paths}
        for model in MODELS for cost in COSTS} for geo,paths in path_sets.items()}
    refs = {}
    for config in configs:
        if config['config_id'] in COMPARISON_IDS:
            indices = selected_indices(config,features)
            paths=path_sets[config['geometry_key']]
            for model in MODELS:
                for cost in COSTS:
                    refs[(config['config_id'],model,cost)] = (replay(indices,paths,model,cost)[0], outcome_cache[config['geometry_key']][(model,cost)])
    shock_start = datetime(2015,1,15,tzinfo=UTC)
    shock_end = datetime(2015,1,16,tzinfo=UTC)
    try:
        for number,config in enumerate(configs,1):
            cid = config['config_id']
            paths=path_sets[config['geometry_key']]
            path_times=times_cache[config['geometry_key']]
            outcome=outcome_cache[config['geometry_key']]
            indices = selected_indices(config,features)
            raw_digest = signal_hash(indices,bars)
            for i in indices:
                sinks['raw_signal_membership.csv'].add(dict(config_id=cid,signal_index=i))
            for model in MODELS:
                for cost in COSTS:
                    tag = dict(config_id=cid,execution_model=model,cost_ticks=cost)
                    rs_by_i = outcome[(model,cost)]
                    accepted,invalid,blocked = replay(indices,paths,model,cost)
                    closed = [i for i in accepted if paths[i][model]['exit'] is not None]
                    open_ids = [i for i in accepted if paths[i][model]['exit'] is None]
                    isolated = [i for i in indices if geometry(paths[i][model],cost)[1] is None]
                    isolated_rs = [rs_by_i[i] for i in isolated if rs_by_i[i] is not None]
                    rs = [rs_by_i[i] for i in closed]
                    digest = hashlib.sha256()
                    month_entries = [0]*(len(BOUNDS)-1)
                    month_exits = [0]*(len(BOUNDS)-1)
                    month_cash = [0.]*(len(BOUNDS)-1)
                    month_cohort = [0.]*(len(BOUNDS)-1)
                    month_open = [0]*(len(BOUNDS)-1)
                    for seq,i in enumerate(accepted,1):
                        p = paths[i][model]
                        fill = geometry(p,cost)[0]
                        full = dict(tag,accepted_sequence=seq,signal_index=i,exit_index=p['exit_index'],
                            signal=p['signal'],entry=p['entry'],exit=p['exit'],reference_entry=p['reference_entry'],
                            historical_fill=fill,stop=p['stop'],target=p['target'],risk_price=p['stop']-fill,
                            exit_price=p['exit_price'],reason=p['reason'],r=rs_by_i[i])
                        sinks['accepted_trades.csv'].add({k:full[k] for k in definitions['accepted_trades.csv']})
                        digest.update((json.dumps(full,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode())
                        sinks['accepted_ledgers.csv'].add(full)
                        if cid in PARENT_IDS:
                            sinks['control_accepted_ledgers.csv'].add(full)
                        et,xt = path_times[(i,model)]
                        bucket = bisect.bisect_right(BOUNDS,et)-1
                        month_entries[bucket] += 1
                        if xt is None:
                            month_open[bucket] += 1
                            sinks['open_at_data_end.csv'].add(dict(tag,signal_index=i,entry=p['entry'],historical_fill=fill,
                                stop=p['stop'],target=p['target'],held_hours_to_cutoff=(END-et).total_seconds()/3600))
                        else:
                            month_cohort[bucket] += rs_by_i[i]
                            exit_bucket = bisect.bisect_left(BOUNDS,xt)-1
                            if not 0 <= exit_bucket < len(month_cash):
                                raise RuntimeError('Exit outside the reporting months.')
                            month_cash[exit_bucket] += rs_by_i[i]
                            month_exits[exit_bucket] += 1
                        if et < shock_end and (xt is None or xt >= shock_start):
                            sinks['chf_event_exposure.csv'].add(dict(tag,signal_index=i,entry=p['entry'],exit=p['exit'],
                                entered_on_event_day=int(shock_start <= et < shock_end),
                                exited_on_event_day=int(xt is not None and shock_start < xt <= shock_end),
                                held_across_event_day_start=int(et < shock_start and (xt is None or xt > shock_start)),
                                eventual_r=rs_by_i[i],outcome=p['reason'],
                                attribution_warning='Exposure attribution only; not an event-removal counterfactual'))
                    for i,reason in invalid:
                        sinks['invalid_entries.csv'].add(dict(tag,signal_index=i,reason=reason))
                    for k in range(len(month_cash)):
                        sinks['monthly_results.csv'].add(dict(tag,month=BOUNDS[k].strftime('%Y-%m'),
                            start=iso(BOUNDS[k]),end=iso(BOUNDS[k+1]),partial_month=int(BOUNDS[k+1].day!=1),
                            entry_count=month_entries[k],exit_count=month_exits[k],realized_r=month_cash[k],
                            entry_cohort_eventual_r=month_cohort[k],entry_cohort_open=month_open[k]))
                    positive_years = negative_years = empty_years = 0
                    for label,kind,a,b in PERIODS:
                        entry_ids = [i for i in accepted if a <= path_times[(i,model)][0] < b]
                        exit_ids = [i for i in closed if a < path_times[(i,model)][1] <= b]
                        cohort_closed = [i for i in entry_ids if rs_by_i[i] is not None]
                        cohort_r = sum(rs_by_i[i] for i in cohort_closed)
                        period_stats = stats(rs_by_i[i] for i in exit_ids)
                        opened = sum(path_times[(i,model)][0] < b and
                                     (path_times[(i,model)][1] is None or path_times[(i,model)][1] > b) for i in accepted)
                        partial = int(kind == 'calendar' and b == END and (END.month != 1 or END.day != 1))
                        sinks['period_results.csv'].add(dict(tag,period=label,period_type=kind,start=iso(a),end=iso(b),
                            partial_calendar_year=partial,entry_count=len(entry_ids),entry_cohort_completed=len(cohort_closed),
                            entry_cohort_eventual_r=cohort_r,entry_cohort_open=len(entry_ids)-len(cohort_closed),
                            exit_count=len(exit_ids),realized_r=period_stats['total_r'],
                            realized_max_dd_r=period_stats['max_closed_dd_r'],open_at_period_end=opened))
                        if kind == 'calendar' and not partial:
                            positive_years += cohort_r > 0
                            negative_years += cohort_r < 0
                            empty_years += not entry_ids
                    prefixes = []
                    for series in (month_entries,month_exits,month_cash):
                        values = [0]
                        for v in series:
                            values.append(values[-1]+v)
                        prefixes.append(values)
                    rolled = {}
                    for width in (12,24,36):
                        worst, empty = None, 0
                        for stop in range(width,len(BOUNDS)):
                            if BOUNDS[stop].day != 1:
                                continue  # rolling windows use complete calendar months
                            start = stop-width
                            en,ex,cash = [p[stop]-p[start] for p in prefixes]
                            row = dict(tag,months=width,start=iso(BOUNDS[start]),end=iso(BOUNDS[stop]),
                                entry_count=en,exit_count=ex,realized_r=cash,zero_entry_window=int(en==0),zero_exit_window=int(ex==0))
                            sinks['rolling_windows.csv'].add(row)
                            empty += en == 0
                            if worst is None or cash < worst['realized_r']:
                                worst = row
                        rolled.update({f'worst_{width}m_realized_r':worst['realized_r'],
                                       f'worst_{width}m_start':worst['start'],f'worst_{width}m_end':worst['end'],
                                       f'zero_entry_{width}m_windows':empty})
                    for reference in COMPARISON_IDS:
                        reference_case = refs.get((reference,model,cost))
                        if reference_case is None:
                            continue  # only used by tiny synthetic subset tests
                        reference_ids,reference_rs=reference_case
                        ours, theirs = set(accepted),set(reference_ids)
                        common, added, removed = ours & theirs, ours-theirs, theirs-ours
                        total = lambda ids: sum(rs_by_i[i] for i in sorted(ids) if rs_by_i[i] is not None)
                        reference_total=lambda ids:sum(reference_rs[i] for i in sorted(ids) if reference_rs[i] is not None)
                        reference_r = reference_total(theirs)
                        sinks['accepted_comparisons.csv'].add(dict(tag,reference_config=reference,
                            common_entries=len(common),added_entries=len(added),removed_entries=len(removed),
                            common_completed_r_candidate=total(common),common_completed_r_reference=reference_total(common),
                            added_completed_r=total(added),removed_completed_r=reference_total(removed),
                            candidate_total_r=sum(rs),reference_total_r=reference_r,delta_total_r=sum(rs)-reference_r,
                            candidate_open_count=len(open_ids),reference_open_count=sum(reference_rs[i] is None for i in reference_ids)))
                    entry_times = [path_times[(i,model)][0] for i in accepted]
                    holdings = [(path_times[(i,model)][1]-path_times[(i,model)][0]).total_seconds()/3600 for i in closed]
                    risk_sizes = [paths[i][model]['stop']-paths[i][model]['reference_entry'] for i in accepted]
                    fractions = [cost*TICK/v for v in risk_sizes]
                    run = maxrun = 0
                    for n in month_entries:
                        run = run+1 if n == 0 else 0
                        maxrun = max(maxrun,run)
                    isolated_stats = stats(isolated_rs)
                    row = dict(tag,raw_signals=len(indices),eligible_isolated_signals=len(isolated),
                        isolated_completed=len(isolated_rs),isolated_open=len(isolated)-len(isolated_rs),
                        isolated_total_r=isolated_stats['total_r'],isolated_profit_factor=isolated_stats['profit_factor'],
                        geometry_invalid_all_signals=len(indices)-len(isolated),p0_blocked_signals=blocked,
                        invalid_unblocked_entries=len(invalid),open_at_data_end=len(open_ids),**stats(rs),
                        dual_touch_closed=sum(paths[i][model]['dual_touch'] for i in closed),
                        gap_stop_closed=sum(paths[i][model]['gap_stop'] for i in closed),
                        gap_target_closed=sum(paths[i][model]['gap_target'] for i in closed),
                        entry_next_open_gap_count=sum(paths[i][model]['next_open'] is not None and
                            abs(paths[i][model]['next_open']-paths[i][model]['reference_entry']) > .5*TICK for i in accepted),
                        entries_before_market_closure=sum((paths[i][model]['next_candle_delay_hours'] or 0) > 0 for i in accepted),
                        median_stop_pips=quantile([v/PIP for v in risk_sizes],.5),
                        median_cost_fraction_reference_risk=quantile(fractions,.5),p90_cost_fraction_reference_risk=quantile(fractions,.9),
                        median_holding_hours=quantile(holdings,.5),max_holding_hours=max(holdings) if holdings else None,
                        maximum_inter_entry_gap_days=max(((b-a).total_seconds()/86400 for a,b in zip(entry_times,entry_times[1:])),default=None),
                        leading_no_entry_days=((entry_times[0] if entry_times else END)-START).total_seconds()/86400,
                        trailing_no_entry_days=(END-(entry_times[-1] if entry_times else START)).total_seconds()/86400,
                        zero_entry_months=sum(n==0 for n in month_entries),max_consecutive_zero_entry_months=maxrun,
                        positive_complete_entry_years=positive_years,negative_complete_entry_years=negative_years,
                        zero_entry_complete_years=empty_years,raw_signal_sha256=raw_digest,accepted_ledger_sha256=digest.hexdigest(),**rolled)
                    sinks['summary.csv'].add(row)
                    summaries[(cid,model,cost)] = row
            if progress and (number % 20 == 0 or number == len(configs)):
                set_status(state='analyzing',progress=round(40+52*number/len(configs)),
                           message=f'{number}/{len(configs)} entry configurations completed; all costs and exit assumptions')
        expected = len(configs)*len(MODELS)*len(COSTS)
        if sinks['summary.csv'].count != expected:
            raise RuntimeError('Missing configuration/cost/model results.')
    finally:
        for sink in sinks.values():
            sink.close()
    write_neighbours(work,configs,summaries)
    export_matched_comparisons(work,configs)
    counts = {name:sink.count for name,sink in sinks.items()}
    for extra in ('matched_breakdown_comparisons.csv',):
        with (work/extra).open(newline='') as source:counts[extra]=sum(1 for _ in csv.DictReader(source))
    counts.update(export_extra_diagnostics(work,bars,features,path_sets,configs))
    write_json(work/'output_row_counts.json',counts)
    return summaries,counts

if __name__=='__main__':
    raise SystemExit(main())
