#!/usr/bin/env python3
"""EUR/CHF H1 SHORT — Pass 1 controlled engulfing discovery, 2026-10-01.

Single file; Python 3.10+ standard library only; no orders/account endpoints.
Run: python app.py                 (HTTP service + automatic research run)
     python app.py --run           (one research run, no HTTP server)
     python app.py --self-test     (synthetic software checks, no network)
Also exposes a WSGI `app`: gunicorn --workers 1 --threads 4 app:app

Environment: OANDA_TOKEN (existing research-service token), optional
OANDA_API_URL=https://api-fxtrade.oanda.com or https://api-fxpractice.oanda.com,
PORT=8080, EURCHF_PASS1_OUTPUT_DIR=/tmp/eurchf_h1_short_pass1.
Routes: /, /health, /start, /status, /results; descriptive route aliases too.

Guide: FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md. The user clarified
that this is a successful research guide, not rigid pair-independent rules.
This pass adds CHF policy-date diagnostics and stop-first/open-gap sensitivity;
neither is an optimized date filter. RR stays 3.0; no portfolio tuning here.
619 unique geometries; 10/20/40 assumed adverse ticks; two exit assumptions.
The old 240-trade screen must reproduce before any discovery is evaluated.

All history is exploratory/in-sample. MID candles/assumed costs are not real
fills. Full normalized accepted ledgers and source candles are in the ZIP.
"""
from __future__ import annotations

import argparse
import base64
import bisect
import csv
import hashlib
import io
import json
import math
import os
import shutil
import statistics
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import zlib
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIServer, make_server

VERSION = 'EURCHF_H1_SHORT_PASS1_ENGULFING_DISCOVERY_V1_2026_10_01'
PAIR, SIDE, TIMEFRAME = 'EUR_CHF', 'SELL', 'H1'
UTC = timezone.utc
START = datetime(2005, 1, 1, tzinfo=UTC)
END = datetime(2026, 10, 1, tzinfo=UTC)  # exclusive entry cutoff, frozen
HOUR = timedelta(hours=1)
TICK, PIP, RR, WARMUP = 0.00001, 0.0001, 3.0, 200
COSTS = (10, 20, 40)
MODELS = ('SCREEN_PARITY', 'STOP_FIRST_GAP_STRESS')
LOOKBACKS = (20, 40, 60, 100, 150, 200)
DISTANCES = (0.10, 0.25, 0.50, 0.75)
BODIES = (0.0, 0.50, 0.75, 1.00, 1.25)
RANGES = (0.0, 1.00, 1.25, 1.50, 1.75)
EXPECTED_SOURCE_SHA256 = '9c8b5279629daee868ad924f52e998be7f6ce638a15e106ffb4851a2df76feac'
EXPECTED_SIGNAL_SHA256 = '9a36cfad51d6b376841bd4bbe8b174bd42b22e4dbfebd0b80e1766f4900b0da4'
EXPECTED_CANDLES, EXPECTED_RAW, EXPECTED_ACCEPTED = 137819, 243, 240
REFERENCE_B85 = 'c-pkRP0u|$Zs7M_yq-417m-qSSp-=oNP3kl1_Q@4F*2|N+lvW;eD_GDdd?}4VyXOp%C~#2r;E2=&hJtbN%7@>|9}7HU;g30{NsQ6umABM{_+3#`~UVg|J~m()xY@<fAjbM?H~U2^C<(gPvfs^{}NNme@XBcl)nP~cPX`h{%`%?@{j-e&;O4R^F`eL6E9MNzoz<?{`24e=fD5wzyHU-`}_a+^B)6$o4MpB`al2SU;g!<{`nvNuKjQP&;RgW|Lc#4`~ReW{ipwX{Og=b``7>apZ@v3{QbY4e+9_#pZlNw<)8ksefeMi`ak{8{a?5L?_c0=Q~Uq^-9P=yzy8O6{fEE%zy78F8`9tWfB(U;0H|nT00Da%^p{ls3L4QE;u6rHzrf6DNh5=XI5J3{c~H$3NQQhR^4I_VfB7H(hrgao@NWxBDpIp#Z2v{_3=m>s3kVSxfciiG3K9VtVhsSbf6ltLRu|z=U4$k)T*T0!T7IoAsE5wYG)i6^9%+Bm|C&M?LmbhNzd)K$E@=?Q>OySPg*01rL7|*Jf6(vY)RHV5N_7bbbUrKwP8Zh@4)R}9-l+=5mW5}_!lG7Hw7z|g2oxL~P+q~odaF5I9BWRKLfXO!*&KJnm5oL7`$I&W+#w={>QU%l0B`hUh$B5|(ssnk&gNzcO>V~7;wo8#TYrCQ7)UJ*21ZY#`~_+Hdppw@VhCsq>CU8NifNFc!RlzF`u#(HPi9<aGnt$hdQ$t}>iFsz;^@dUX?}3^XnbarzGgJR<{Dxum%lo{K&iUZ)Gouaq*UhPVkwosVhSl?dg8%I?7$fRg)JE73mE)0m<)$98$b8BqGm9V4lt3Udx`@`w@Et~deO{CHDq$)%fTu)SCa~B%B3J>tP15JsY=cX&f^3dQrH~(_-;DI!9nZNJn3H$62u3skO>JaDNZ%gA$y6lK)cMwpAVbz0F<D(&`Ahr1*&UBjzQt<;2BWZ?ls)n>iDdYIctZ9I@@ZvfckwLB}DAW@S4ngEjuza#f_E>O%B1DjMO-el497DWF6y1_Tm`x@8O6#6D^vo?<YNK?jFthivNh0SNxFDq;tVVR^p3H=gX`5ko))UU(w=dl#+_4(f)?dIfR+UA%+yt=-RS(|0IRhM$&9;1nrGA{OHiaq?(b=KjUFdAJD1d<LxS_o=$W1b+QsOogt=xPLr0si4lR2j?8e>!V$#C&#{+Q^L9)A{Fxtu((1g!TI%9ZY5V7VD@~w=xCEe{MG;bCHL9~3sWrvYJh*?)+r1fjZ+_axnpUV<Y(2R;22~g55EbTqV6I1mxkRPIYyF~Pd%x>+zyIDcL&8P=FHuF$ru-%G&3PN*=)9F9qqAir5?)*?IY&{3mf}F+t5F7|0@Z^mxByi<34{pL5MuzUb|*2x22_d7b}?qVfQ{N!HNBblf~G|-(&!&~#ij+Dfdl;!YsjWccebcvc)%*o2ds`P*%eN+F8R$UtaFeB9HIzLy@eCr_d}{XIB{CAnzUdQ&nCZq-vf+0%kZh-4;DaA8bicqh%w;PrLxZqR)!luX?C4!t{#zIW7XfQ4<>QfeEV2vg$Fna<3C~!@jN<Pe5XGq>Z&&NQ<~56{o{q(i&OEs(>~f1nI11t7=>LQ@iOfCk?xW|0vZ{fCYlV7KsD~708x}Yv|doYT5p!8tg`nPfxTzlelGXRI^~Gf5KF*{kEmqFipImi5*8^>&b3gf#t~}aHcH?${-G+#aW_~z&P=a<p~C}#llUXWw8euI;{lrS)Jzu7xL<zBBrigY{!u&=={dq!WrjFXnIYX=T`d+%p~^_Av&smWFhlxd#jBQC!M}yJkF+>Yr56JLM0AE213J|{l`iRU{21C~^J8cWqcmHM$=8cZ-oT?PJT5@hdxu$X9cDcp=H0`Na0y>_nhfUy)j(K0Osvlmt18M2MN)$f#7}9h%#TVzM+?%CWR7mKQyko6UAoVaiQw_LwLWS{Iv=%hhfHU!9o1K@bKwP^!g+z`aC0z4c!n4PJck#Auu~>E;^Vb4oqx~1%2bZOcv7ZR@qI_SG%v$5k~JfgC?oU{myi+a_8CJ+YUmA-lxl7;<0=Z)H;S9;%0#!1-bDAzI>#pZ6bClwl+yN8Da5BrU_MpZrsbvQ?%%w@5J+6|2b=$FeV(4$`3t3}65%6W8l7?!H|^hZ1f+&|K+22<No*dmw!h}pa-Md0s>f4Y&C2l$d)Wy_c!rolJXp7nI!$@Phl?hYIljPZpDqO9g|`bt%GsC0kt~?Vjgk3?OTgri?y?zzm_wC8WTP_K+F@X(XE9gF&7({OxjE*lm7mg*q@I?HN`RS`9tl#8XsOMgPP7gw?(7eyu!f__gv|jXYm)&>c{8Vq4JCOzs=C61aHC8^TtYkud;5cla*PLO54vfW!@~*Qbb653XU5w3OV#5U5k!sf9O89mtQ;Qn9G$GqGl%8}c=iZEi#=*7>+M@%ixw)Me=bL~@`MnhFDmRG{rT!eEF2}CKVn@I8qyu1%;QNacv?<#>Nz92s!Ny($bPRtEhO~!JFm`HcYJTm&nXVfPnW{Jwl38$e$T1Q@q2C;zR8er$%?_-m(ixmwRz52i<E?^Jf&688|(J5E@@Fz3;z+9m=<j_KwJr;7^BJ3TrxE**A<q)HP`xrG2-gqzuHI4OFA_QP(R`t)2aK1LJZ+F5rox<C}<zUl3f4#{069n)n;E!cKlZ4_)<H?u^Qn6n{<<B%{I@P85|z$d9ycdl4Ny7@+j(_d)lQmip17V^+&vPs_W5@g4=i0e#-Rblxd(y4G9?ky4hObprXv0C*n-h3qpHE-_sNkN>Ojk+eh-`=rGqIxgn0p!M@U!MWTFK6HV5sU5hrnXC4)oztyFh7P?f^ng~P{(jT$LMD{s*?ga`F5*42OM=tIKh$Yb*4LXv&omP?LnXoH0oS3jHx4$xBTW!BIKcMlQ-{O69=U3|yrFuP6a2udC@0)Wy{_iLudqS(Eg#%eq0Z%LUrW~Y*RzBV^B3g%Z`}hnas94iNGPE+;lC!2jFPy3Zi=edt<t39ECCxr!44L4ACNH=_GM}lgY>tTH3uo%g7|uj0=;F@OoBEcIII=T(^nLAm3}DoXu90Y+!wiJtJ)T1Ew!Oi^<EbMwb9ma{^T0Lyh--+ao17gUOkvID>@*{~77RdrLtV%>7`KnCOCBZi06*dq^C<fR)a*v6E=bv^OJ<qfy$Metc%w@a7V1(~H&k);mJ^;KmH<z&{n^23(lC^OB1UCW^(vF(&5F!MqO9<^nwIekH7fY|h&jfyf5vHtB(Cg5G%f1rs_CGh+?KL5XRW7UTdY0~&6D=YgubG#edOW^)uZn`bcBaEa)j$)j<+sOiF^2)a+xctx}vBS1>l<nhm>l<MH@1DuekSa<>*!CBM!`Qm&$|tn<&1m$kbytO|UK?BuH;2GFre_if=?NEFKTu<P1OJ;4<&h{R>7jALYj7Q{(b6+&aj2IWcYSam5z%6A&{#?b>dChkQ?a`@^_CM_oam0I|N?zv7iL4QUU+Xcu(Gw0b4mM2mY7l?$=3JnPBGg}#3PKW*%;rTWh9p|*yW#JcqrYe4Ie?v5Uf7<&b)iKcu8)lq(OVMT>rr~)Dx1;;ay7WjGn8HFqB6!ce|*F?Is@!IH5Z@iC;QIpNS5OKD>%Y;*o2Vd!ZufF-6ig`}$#mh&y6e!F`yi}MU>Fxy#1;VPsCY!4cQLL%PoC-cWU8QKv-8ty-8HqflC5WDuka$|+I&mjj;%ca_v<@lmXko_aeD!zf$#cYW{KXYlnkw$}HepyPK~<EJ%P97If{So5KY-9W0{s`4?&`$KHN=r}9nxLO9$Kgo6$CYzP6a`bo$c<A@4r89ylB&H-X|7^<S;6+2A$$SgSxbCtLd@XlE@q()j5(!9_Qq$K|QE^Gm>YP#ViH?U^sqZ3m?`6zhAM0cxZpy_d<yfPai=u;XzkshoDs+HpAx%xw%I*tB(HQM}sFYds><7fR5vN^zfvk^L2`Y^EIXl?COJ%QkW@gGLqmaqZb;owM=?_X?;Pj`p^$gitXH}^T$dMl6=KACZ$h~UFZd%^#YL5Mr=skH+nH7>4n0{k1)yEWL*2t!?*?NBVHDIoYH+ji)YSsi6@N`hcBK~*dDvo=Sh72$Zz0ToU`^(mv}TtNPoq6i-)#&Lbtl3LxU$5hbO<SJ+jgsp$JdD$0N6R>JCo{L$oHFLo{rWJ%s7T<{-(d^=7Q1e$<qT9f{L_tnYD}NU?JcuA~m>?zN}=rLn<IK!y~GQKaS%mN$?T@<`MUfc*X{yhXCirJd6K6KeaF2q6*#Fi5OrhF3aQxNvfhg6$(L4pffI62IaSFgc`qCIB~Tl8l;wElX?{IsTqWPLLgwoKP1wxE#+Cp$#r}^2Wqu^!jX>q#9pQshBGY5IvLpW>rg0?1$L(kJaNjhOxz#Wr#Jz1GJqg+sta_5RYl|Zl}rxrekQ^uaU??W6n-AhlKBu@GX*k9aM{iOQ<oB=kDAVIH)~EeVW0W&pdsk*+OG7tnp}+82gBGjHm21281-kbM^>-@f2o<2kzc^3S^PeEVlOEI@PBG>_#h!nPA7{@9Un1&{%~|aiBs+4&%XnUE+LCDkk5PyfahoM%!e*{Nw0@c?3xOQ%?pZT>-LHmj)2jb+ONoqHNpZ+E0w*^^{G#p0fJDA>SzxT^rxWtbr$t5ZR@C`rE%hXuXX~qZf>ixCV?4>Hg*_;Q)m`)FB%sVy{L-uMUaCT)Yb5NPS=N&@o?4BNh4)uT|(H<y~lw;p95(<FV$@JiC++apHIJsBb)NAMIFx@e48E;=z1}r%Uq=5635r*_^OGE6d_(LGzo5oy<u7l5Zauct*9MMtInbMZDs1wTSCmtmgQVuUw)OSP#?fg_f^FlJX0rvVREcroMVo`3NpUMank1oJi#-PlL##)^e>#P}H8Y;xf<m)g(r4&Z58S*>%dDzc5Fc=Z{!oJo!O}u!Lo+?O4ratJq>A>kc)9@a81wK9SRVi<xYlYX5~}?qP^$h$B45Sj4o`o;U$UQ0L?_IZqlzU$DzEM!e2IB-^p!{Wl#UWo&qcI5IqkbZeK1;^?9wnC&vLzqXRft9Dt_!rExk<nWvmAt=Ceig}G^JiBl4pzzH@4w;7>_15O)W~Saq=2yVz>e@OR18a<@o7)uPF%#PY?FmP>^e)`l>7i^J;~Fv6HxuqzM_H)B!&+B(5Tn@iBhDe7-uS+?Hgxk!isqG+hN?|?u;{DCSXIR1Q9Bo)|H3hIEi%TZI55WNNVcu^U>ueb+A;*HPI;ibn%F4F?m41=R8I%p(6xyUx?#{)Izy^E(}S@~h-b;@*YvO*q=g{*YO#Dfz2eVSVTBc*9&e@?PZvXor%QM77HVvlt1-)BZ!L^F-u{qHS)>e0b&nBz#7hu8MtsD}$A}@N2M_AG#AZ3GG0W}z$1hoQ-w;?*3wv`P`GWkk_dWMPz<z1L_%HIXYUBZZ#3hDR_klactW1?S_F8|dNiDE_I070aweY*wLZ#+4J%+UyM%F&s5zzhj-o26X?HlPKHP41?ScGAC(OhUTM#;9tVT&&4)%pAhk~2!}qg{&Ee<2_KV2bbzF$8$nI+JTWsl;X}%`^I&Z<t-K{(9fPaN$c5KSoNL@)eI~j5^{7&k*MT&oQ!pTYvx+D9&=s)1Mn2ba<M-_-2l*SBCnnAONy3+hgrpjqr$5t}7lnf(_W-__4d#_?R2t(ZQ4-7O_(A-Y%M|s1oYa4JzvQ8?=vhj(GpQ^<J0itu9qddYz6IjZ^j_w$=x1nk=Ent4FO}7U1!??u_vZ;{wf(xP*8hJ*xGQn_3_9_Fm8x((wzq4#6r5m8qgjCVFF?T}P8;@*qVIDX!6gWNI`3*m~4yw8{UHNzT4;Th0x2S0?$mlf%B0k9hg2^CR8aoTMy~o-|pW^rT&2$<{WD=2s*5PC^!VYJ?}q|9r$8;xSzZYdl(FJUZ8erQ(L!Bu!M`-A9ysHA^4QxWGe)dT4})oN`_9ux)8s;t`CsN6>0dvU`9NZ-yaBNu^I@r9$cHv#AKJ5V=8r=dYBgCiO?Wu1OtI-QH7)OK5bOY&JTH?eyNnOt-yp(A@6SYeaCooj(DjB`=m*)zuMGJO6S9G{h7D(kCHB;&=m3lg$xW^0UWUELHi<6l*8pFRhYzi6^eL{D?Ki!`la)ltVVP8Z_dL7gV_)4GGYWmo~S2>{fe%r>I|<(r5WB@RTE$ks~}qECHUQMLyxyK!*imO*R*dCATjwNs;`j7g#DS=v2GZ6Y|2(o$1s#qWDNR&lfip&8f^K38{`5K2zUP3-*HyqBTQZDVj0IzH4?AR6T)J$LsYBtB%OV!LnnH3@m{4+`1t(dbn)!I?FGYZC)wOPhpU95{s)`dz$XzubTRMU&<{KXB|cP4{&J$U$W4fqtW3yI!a>C@e~K1<B;y^ff^8uU84flH4+Y!j7j0T9PIb0YCX&K4XH76%aB4>)H{(H;^2ns(z=bsdZS5bqgv2tY*fw|lWH#JrMN?1AT^pYEh$%HcKlenIfAEnc?6FQm%Y7bODY`AmdrqtcG?nII3w-%OCo@Nm}dLv?zgeFGurq56>CTY9_UWZ@%>h_x!<bTmyUCIMr4TkW)f)rSB2gY__VrJQN4%MKZ-}hZjjc5h+PPGMZ`}gYNiTdCaB5gOi<&Y?o5qQC%;r1EI!lPKkAAJrYP0%5ktu2K|g%^nHV!cFib{lG&6zvrc>SAL!TY%AJOCS^8hA1*$v{l;>mUn*Yc|GA!T&cx2t_hL_?pGWV&_Qzx~yXJk1U8d%W96XNG_FQN~86#4*Q^!agLcVc$*@8MhG^x^pe+xQkw!PtdGwNzI8-c{<od`(#(gs$H7kS@ls!QA5v1i~+0qWOZ7q;oi?w#h7Px5n9EkjovN9`WBJn(+#pMI>PE<orygQ>-OB$+jBQu>|M68GKR3`od*pbIK8^x;dosi|J0?<i#h(93;1(4#wG;8>_;qXJY9<0a;kpW<IQD{Mw{%KgK5L=W^r?iNc#>6tfk8E_MYGp$^POOS(vF7L{L}?{3BjRw;$;?cU{5(T|GJ}CISJvi9lW$52(8G9Mu~pJ>20O#b+P!GCn(`-NbL{U>tQQny5o<byX65q4`qlSllrq>AM0v5<c=h5+PwXar}yej}C@iE=|MI@iYzGn2YRF@grSC<<+J-t>Ck2+B`K~f{M1&e8nXIbv)y59e@%pr%5JlRMdrGT3<L!EtX?<nA%6X6AX?egtk<30y4xB06C<)1~H&;)MgV++5%Kub2mZ77l@#5=xHB$O{7HicONmtM9PDqNMRpy6HSFKVCQTA(Hl6#50&p9+2g1;C3hb&1vs8153_~^iA@GGkX*$a8psJ=O@S2<AEUQu6mf|Mqj>Zqh8WMj{p*}-zN8~_-46)bS&@+Wn)SMG*G=3PKO`)D>eNFwuweD;V*5vLeZg1JU16Q#;F^L*gGk!nxv;9fPoAF)Fetd_RC0NB2vTA8+j#m%d(_G?D^nu3??=3L`#w_K9kp8GqgLl4I}osAY??MHz4_)-cq}|Lct(Aa$5W!hsE;^@czQZ)r#%`^(`vGLI14C))t+4G_l{r5=m#E_^Yzv+Vb{PtF=02U|H@=YH~tLN_?589%vg!jp_`_&q<5QhOU?iuPj*S>v4S!j4D}IXfG0nh4**rdxU<RTxU)AjIXrkjE1RS+LVuwqDDV_f<prWDb%luF#^@a4(CEmMN|)w$mQaH-D$n=3x>MEMrB|C(iY&^=`eQbDJbIYb9N`&a3Gk@Rk*%++xRs~L=2jjMD$eL!dsSY+vskdxKk^EX4VVlblpq%Wk?vk`+AqHGf5x2pD;~91mQ3?$@6v?d^~*hDR3Fn?IE$Jvd0OR|KPwTf;-=SLX_X`TtlJA7<Fi)5&`NeWYF|#39&S8`1k4pRli+yKe}YRFWv%UF$sSj=qvmH^)h>&ja8<hoAsA09@pRg3VpEEjWB@AGU}w>v9l{o!dK*>1#URI?$`rQ7f5d@}I;ERh&K8D-<@La(ydGM_VQ;w+=>HZ8E^f7gi(75vjPH;D-XVF=zYDA}5#va6wM&~CBe*>D+;7W}RDb?7iX0!o%D$MYe_X<f)6GBP$OSm1J13HEra%{?JpfL7@P#GTrH-!QnMft{eq--44X2iT#3dktws(9<1Zz0drOD=@E<nk?R{&<=f6v#izWvI10sW(Ol8&u#DNH|q#7jf{k(M1U2*Y}lkMC8zHbc~dkl?iakuFtC)rDvk#|kwAC)<5ZGn^rg;AGpRxrQSW>ypf)jp3{{nRE%SqmB9LrC;?aEh4W<=+}wVW5-3f4&o!u0WGnGj%!*Nr(YWhLyKLCs7JkT2DG&WcCP%TeXI**(j!_6m1&40Wl~#Zwl3w&DGW!_7-DiM<rH_;dRJ-C)#8+?#k9SYQ%T2Oi6^v}{35abu{c`idXR|LDGq3zLr%7@@`CZRvw(eeF7DYGus&M{p_<ywSQ%S5t@<z%P@7}!o`Tv(H~hFBrK=@s3HpdRpf&3CwiS5{aSekhVL(?>-JPHphjZ1uSn$|BIv45qc^DQje8dv6!3X_7wLn}AGpg?|=PseER)qTd*uP5l!?N2)Tb_vN=n;#OYhUpyxpqp+zJ*LUtZ6l=wItcPjY&Yu!^mIx(=Wy8X;FDq@KY44Vp_nWWB5dlU=6VZu*hyaTz9)@tUy_31xk`^i4d3HzjpL|@PY{2{-6G%IL^&}#gY2(lcD|XSBFo}O=gOsbFn*9Q|4<2;tMx@5kD}xmSPvt;8^w*RVsYNCBP%L%wb*9L2(lT<*=kf#g6J~UI0Jrc4;AK-+-jeFJ*eVbEVyk3vRXA=llNC%i-jNYBj`>S`}NSAM{OO+%{!cm77%ushNu3o9uoUh^tfVUu~$?e_^$Dcg8csky_O!`>dOO+_aU6DZ5v-Z;Va%_|+%ahC=+NmHyREOvbTNF6wFdifIj~OZP8oy=O26bf(##*4_#5vx6rz{R5w}1Mm&6$JYfrh-Wi=#es62(z>l%jLTrPkm++t1zo!o;tLuD;MeuGuNY9du}G&FV=D5XODaj+C6%(#C}c+^zx#v)3C9D90n{Hc4Ins%D@6@=U-9Y@>6GT}!+t6dH`^C5&Gv;nAffVRdLNpT=|`FNubDg`>KR3Z2+$B?0EmukwrsmPsK#9#IPd8LP~X*o`Y?{geknHQEGYCCSMe2V{eLqqaNYC>eZ>&qVmsP$og~2;CrPlFBnhhv`>$m*FVxXA{)<4rf3-Dzc&u*3e4|f89O=`sU_I>`%QT1Ms+w#rTT2d)Vum*-qd<=ZX@Li9p;7;ZHQpln6_)_dA>A8n;@Du1ZIu^$WOuIiitGGli&;OuX>p+mVqWrzCRBaJIp#zA9=v};5w&F3=Ud)BTap$&kVxSboN$}OS6o9llpahbh&OIDSt^!~-t|P+hpVxbra$;8J58`K+`c{n$NJEyJ?|^lHM=2&y<Nf@+ogGqd5>hg|9I_nxO=yiT9FH2YOa+&&2GcbXaLKrWN)a>#g~zx@_R8SUU)<@+X*ui``b*$-)6hGq+Q#!yqQ7iB+5#kDy1c#8YSI6VhQ;GJ>WA|(yaL~&bEqku5OBx;KSFF1YDbYs04kM{Nn>y7K1|i*9t3O&G|_f1M6Z6fgwJa=>*}VkS3cag(ODTJw%mq@K*VU8dcesm9($p8SDxO4`(_>SKmjxy!!C;#J6uG$noGAWQ{iZCbE^csYiNmKA3bUD=(^MyOHAjwWd+VqAIhh8B`B4O;jlKqG-%zyy%k4coW?ka4lS0j6!{a-r0ol1k&MEI!OI%@sM)-=JpkH0BJ~fH(pbjct)6-+yGbC%*aSj-*XFn{#lS7^QyYyRqZmW{*%R}-zHw-S_H1V4(WD|3g?@dYPncmDaYPbQ=RhW#2M(d2R^ORz83YYimmLMSryktdu4S<?{B~Kw^f7r--qZzs{iZpclm>-e`0yHW)c9QF7O~GSmA+aVd+<lAs%xumw05fGcNham@jq#)HEberT&*+-@fk?y^27+12l6xLr?1k8zri+U`KYe;rRKc$K0}`bjOa;o}E&HP`A#$@GrZ0{YU4e@@kO|0HNQR+`f_>jD@&}MJ7I{Sk`>H6gGZT`++S?(1=et^UxnYCC@{O4`ku=j6Vd#V+o4^#NslVbhhCKe$+?2^rL>H_cPYo53DJ04h|X4)~T9->7AL{!1=d;GOZkgOm@Iw|BZLW6?9u%K|k|(cN)}7k3v9j4TbxMF|Bq61W``>F6Eo5JO)-)CMk~*I|dlJO@b4!A+7<iQDCvvsNvYHfk~Q=T&-e{T*=mZk6+2P<_x8e8!wL?&pgJ^8eDSrEjnEF<^T?H4sn^Lun1R7Yi3ofYfuWlIzy{rpp|tJT3IKKx8{Dt%Yyi?G;hs!@PKg0t`7+PgV|7YxL4^7lO&4~hQ06`s;82VLlUaY5JxIA3YqKn($As8q{PNynv-*fPj$WY;b}2te*6ptnteBSe+^^yyu^*aoOlf}2fXyCKHVL$l5*?>wCHY`D>yWaoIjB9;WRBR-G*b|?aevqV{~QlBHX^(Z9L=GgUl~b6S^U$5FM0#=+cMphrap9*M514bH|%E_>dNrEZAqirPG6aM5~M~M}}5$g}dViW4XEdhPZ~b@IfS#Iga!)U$<iQ4>B*-ZJ*c-Ap1je)f8^wL6w{qH{3BHJ9td^h&7;6tT(ZyQYh4+K2<(Cev+KGnXiBKy+XC1u5u4~l&R=r@1Ut+TCfu=OLqCjbPX|w>`cGdnq4lTsXzBvTr^4(#<6k6YqcW3p=VVVX1<*BLuOVrUKQ~bmw;8>FB&<1en?7YH~nn<pN?Ktc{h&fIWJGALZTHN%r~~Fy6x#Ro&H$5bERa=Em~W0kvY(7E`s$nrRVxzO+&T6EsMCN+eHVcNBbO`sZ$)7sUh7P?-T>d$pFeAHkZO$A9m17i)IU-e|}m<-_w^XvtHfIaXi+e#&3Ebu>`!}c+=Y*H=&@N?vP#E>G}uu_NXUU-aK)AQ@sb&zgmUjp9zO+d-M@6?a@cNbJGbh3|^?u{0;6sLa{D8_DX;^(@>4dA_~Tz8L5sH1Uqa#;^hvTQ=0c_D3TPj%5@I(NZA6SX{r1Rkubve+hPEL+aY@X#&WMr<yM)>rWC4M7vRu3F%U<}WZ#Ale3IT(CP3fk3q8Nip4F&CO%bahhJY2?*<WjA;u7Xmh}E1nL2H?4T7f`s0IIw=ZSCtqndq>-7y%k$3INf*9OE#(5?^&SbL^43*Qwm6n!Kt^xEMe%c2Id%$IjIF;0<wf@OWQiHy-c!=Jn1#-;oVj+F{doEfW&z3J~!U5Z@@%5K|0@_olm04CpN2yyobt15~lRJ=&|`5LukKzL9eU3GX4{Eu=2prtG5elt)gf8ZA<n>DBbYV#wappI20>F3wiId3^eab3i2@b|`JBgik9fvjeGSv(`v;zQKeL78gAs)fJPtZ1p3qArpLX(bw2%tT>zFc2VUx-an)Bu$i}9>Qv=%9SSn|L0)}Cu&Y$%BWreNp4B}@U2*B7*Cx@pcEBXuq+YWlE?0TI$L>>0*FQ#++9PrqX`yI&bhsFD)$U-K$qjL2l7>`w9;IR!zcnXmK7L%B-R3d<{kO+2uM%$kt2!!U1HyV<)mJQQD(Cuyy`P=J6fV0I$koqI&d)B7yADx6O5@oSv^RD>W>*&db?s}dD|Y>@Qe$>qTtasAB;ZvE4<k)uDU1tVigUrK>bq{G@S-~bC1)S-_E8iiK7FS+IDK%$DBElk#gF6!b6WQ-z?|~|0zvM?6&19|S{u#we`&myJi}EMjZ*DvS>Qr9VsOGW#3jUKuGAnu5ZWpdHQEFgS96%*-Bgp*mD8Zey23*#dM5aYm(K*bX9w^>85jd#oE=DW`h*$DrBAfH8>BQVw(vBS+!OHljT`~`h;IP#zL0=ReAks(e{ynom~(m=&zNgx@l@wUjXmNTS)-4b0z5;y#glT(C$aGbKvV2TkwSe|UC`f;>-neA>-Y#=5n5b&MckN+V}W*@<^70DfR63HrQp(w6PG5{#E`6JVb<FUr&<ttl~0CtQ>Dk&yBWbnLavTYU4~IUhU!4XXoxXjbVNMcr-F8)-DJWu?(8?b;Q;jH%z{aA4F~y{8>3O|`w{1Wi9D*T;5gyU*0+ReADd{jzx<L3rBPKmGihIWVTSZ3$^8*WXG!;+Dql}$ttVKmTjcY$nO}XPq{NF`tpC^GijNsTW%+cmhJ5Hju`Y33_|<M!!$({$5-a3~ZewXN;7{f)!((2#=!%!?qRZzT@0iyRN4yRxZtYHrt0yR#>Ip)2)>$CGtWaals0AdsE&)YD^}k|?0m(iGKw55YFExg_x;Ug6iu$T!-TtolZIb;fr4^uX3hGy!LqGrztTe{yIG(l+^bh3UCCfEXd!e_9dWVGvbgmCGfa)>SBy_BYIC8A(amnpX(v1eO)u7QBIfrKHsk@k{e*Pal_7@4*?}=$&v&a%yz43*rTQxlc!@7S@YH?8<$}$!U4E*EdyP;Y~QCEOUSpkX*;y>aX0(w&NMww%yoylHu;!6SQyAN)Jd8I*^IXggbgrmU=!AD%zfVwn4$c4a-27%Qe^tmH_!JVe2AU*>^ot1}zM<+yNhfeX*4jo(Uw;RolV{voS&+h<zr@*4V=G}X&`A!R+A$8S%a*pl_JfV+R13Y-VV($_}nBoWwlZn7u$Z6l&Kq@^n)7OC>`n0%>vhc!l%;M6s@)LAo#4E=Mk&kre7y(nLPwhgPjZokBW_iI?q24e;Sr-r5{cHAs#MM1afQGmRfDY+4q=(VQxCSIN8zE5lcnf^3w|&DglC)@DYF~3&0qW((IR@0l5(0v{x6<d}{`H9?G$)Q-=V_n#{%R2t39gP<fA{o&^oYK~Bc^tI*91syWVZ$sKR=#J{Jwa|b#^s_mY#*Oxp#@Q|FYU$z7g*{!&MiZIPI$o)#N9b2oGFw1(a7@#ctsVUO*tnPayV+lj{6ND*A3LtFVe?+t(!^<jAr9h%p3Y-UkA|@g@?<HWCr0n;bS(d7IT@B1MbY5-eHe8|z)-W&rt_JGhPe<tC9NHj$*+#r>lL@z+!q>+Jhi=R=MkruS6zmdca#W4TFF7n`Q8uCekgRE`7K>-1R8k0~u9eq~+g&X_7l5hXv9@JgvZtB}BSQ;BR!*_s>0?tuvDU0+*OSAfoK5Ec-IeMld12?3db9&14P=CLtr2}?*6*wxrTdeaGDa#m=piM>!t<55)tHTsHIYBZz-+X-i3tK=b?2A}3Jtno53D36`OPg!8pP_xPlv8O>tw~R(KhPVVY=on4CnUtF|gbTTwY~Hb$Gu3Q?WVo9-CD%Wy$}>W^n0AVz>dO$yb`H`^Xoxi+l#f(3?ery_&(%cJfW^#qe^3KfeyuM!R_@L;N?x3v5nqKg#1*YyX~_Q7x&I)J^@Z5zOS{0WzMx#2LFxB!YH?M3<c24v&)aE<;ef7P<st3qv%FIkj!g^CriDeVs%U-vAQAXY%IzDk=wQ879eaBjI(0h^Fmrs4s2Dhw&F@bUEz0nS?I4Xbo{aHEQHD5D6t%l2*H$-EXmm5oHdo1--1_@t!{9fzFxc1SW5@bYhLIu~#T9W}X%xFRX-$J-8f0kLS=)_9`onS30hw{3&17<3=t;FLPcxYzj*g6bAzL3Ajn9l?v;t^x5Ak(B!oO+OXhf;H)5KyYWt~5?1U;M|@v?_=NVi3Zho2&`3&SC$%oY;!AND7%A!YD8etS~LLFy&qC3bsHap3lz<Lc-|lOtwQW^;r&n)hUrUY9=v{8ZL}R3+yU=V~`>rsK&b+|m9O2Zs(GEhA~CCq8uZ>WIe0lH%*D@_jc)Z}$B8CzfZRWU)(12xtW=oN@RSO9-lO*}$#cj?W$4oVyIphC?*9{f`cwkgwDf;x(n{!Qv~fF(ukAZ0)Bhj<BXyq5i>in+?i;I6ftSGdh=MIGU_acszi1PiKAgf5gkHe@JON{W!;4+B2)nms<6~`0w4qqJFt=DXDlG{h2(NI`<JTr_P;H+`ENQujV0|EsvnRy@r4NXup`!GdNXz99{+0!;$!9^ed(Sj@Yi3H5`EuPOdW?wQvM6JzJ!~Q>3L2L21o?ab#VbDUEkRe8nXI^(?rM8f($KAKDc8^uK`fzw1Tc?7S}aXCymiZv5JCpy5}XLsb2-VrPCd+^W??I-|17GqO%s{qGI)&(<oxM9Z@&aeMt&9G$mfJE+%eM8bPZo1R41yO!cW;j6(1qymfWbp;oo>M=?HB2Yt&0jPS+VBhQ*Z&!AB-ioo>1#B>{s_D&qSg-$Bj0{c4D>i-Mg~V(i2fp-2y0b+!T<6n7^8u?Pdv=9W%Ad~oh~dZr4p9WB-ogp5`ytgGoH$)rO}em(XOrK)?)wNmUQLek=c*6#qA0BY5o3U-OJ$!gtPC-q@e*+Xl&dH7r*pnAnq4J%HRg*}cp&O5{E9im^XO>to%*1`V-rH<TB}-LxV$(mTEg2$n>f?sWhz=I`V}vOt{>?xB_yNjVa2A2Cc7i!<@mxzfWqyM)(OJi0o!gvP5tl3Q&ib|i@@Hm)}Kbf9HHWH@W3gBJ*u<HV#$h>rXWnH*exn8)<*fWio*zxEXod8oRMDrLWd`4#P~=tZSf$*c;-!+LN$}cGj5k(GRcboqkk07<mdv7Rc44Il@Z%(w`9`%{ZM5jm04xRPT?z({(|wUVOH=jr0pXu4%E&5`L7rQI@La)F6nUmMA~HY6KS&?nytp<>%Ap!;L#Nx7oO_9yR5hFvYzhp?zKm_gwH)q2J?a9YOi%+Vttlj?fH~;BFCRA%2Qk``=g_*e#X@o%0qmUo#Nmo>(YIOOwQuj^;yH&hZY%^$aL7+S$)+!7hd2goELZwHwROM2i)H5E1ogG6L#7p;il&%Q=GM_RJ7WZ?n~$X$d4|o6>~+_h)mg}MbyP5M1;D1&uIU{Yv>HMA63l_W;{i~`bN!DU76_i(VOU=R_EA6pW?s<ol@E!Duwt^&Cg*X+2-UWXDM&qVA#vyU5!t3pRLc+6FWyO;M(SV#7mn~j*_SS!;ZkH;$FMYqB9;Ov020_1zs)kX@{qJJjE5Vlyj3wkQw`kDa2!jQ7!SHhOZV)mdlK%+II^<c;Vp!k#hD$a-;0kFp(Q0^AVSTiP#}aYbF@31aC4jDr5IH3-GlpUT^105i}2?zZepaak7tSjRjDK)@W@ov^1VS`4#7Y)*;<3u@p>UVMmkAOF!EIBWsfZOL;S++WZK=Iktb)6&|~p!{9-PmFbXf?GL7KYjcyq>_H>4!@~*Q^m(vpnlDJD@%nG@j0mDecn<M8F;)%_8a8#_$IlDR5AYm(!RR}>!bw^09tyQr{bCC@6G1BU^n5Y;O2huqpRZoc60KDDigitBNOy$tIYu%l+O2c7VMcVdm{8NJ_dBa!Ow~Rv&KJO~`2n~yKLGpT67A=u8piKAl{tRz3nxs1j7z>2pYiP+slIH1`cO8?mFOw1;`?*@$GXHtQH}gZTw+|bO#yLlZVzyAMf0ROOxKl`{PA3Xh)(s-U)Yz=FX_}MK>dhoOsDSO2{D9|rzR5fOo)Q^Ei4JZtG620h1q6bS5|)JBiyKwovtv0!Usm_CefO0qBS!(JlGRwZyF`Z>T2h)x%W0qt>?<pz^nd<mtJ)}`c!cHjLP9;(s9Z(P^5+gjDy{b?X~RlZi#%+u{4nS4bS=%Tv2b$+eh-?=rGqIII^8@1|00mUwI6iUve8YGhbE;HoRvZrI)`|rLhdi5UFWRBpSc<6>CgnpR;F`9Qlt;Mw9=@#XSJABzmK5NBkbH_EC~&!miG6V#2Nl|H_1I3I5XhfW~)zyW-8AU#&-!Dm>lY*}}6_OYYMtYJS@BRno$LEXhv&J#x&wQwJ%6m5(Qk2v*+D1MI&aIfjL3U}du9W@(JU{G|dFSj4Oa?AZ46XGQa)zG4iS;DgRDxIw}^6B*^C6A{H1j?|ejoQYJ>#f_ym#VsFkWM<Tk;an3Lb)st|T1x2pK=H0mp?BNj7TXe<tE#Xfg(>n8KjIpaLfVrW;|A8C>Rh{t==w4M^$p!2--C>ORNRtCG;#DRE-{g^KTQ}77C&S>N2u6?*7eYcLk$Vu=%a*%!j#o*Rb1uegr~T9q*pw}wr~eWOT(xFriloLc2%!3N#3l@Y$VDGk1KDf=R^pg8$24+{(Pjn78S&}#urg%YmBb04hr5m#hEYvsPM4AGNG@nYah9ILiK1(51ry6j-28#9&+pMl(@~mKgAh$$JIymB-op!hs`BQxM);H?;-bwt{lDae8ho4?oxShnG?lV7r|_PfL+%mBuH=OH2NNZ;yaa#-BZ_pA>QN<KjPqK@6!EiM($(XWB*Siy4iu@)^EN`mGSkD@i6fOT;Fu=z|oiib^&)(8}xY*>x=#?Ug_140&E{vd+C;h!6?e)IJ1zVi@dNr>(gjf4V%x@&beqAwZxCQz-y_#4ScAu!6mVteZ?B!I;6W}NR^^nj_OnhLv<9TTv$<|7m9=Wv@!RL*glHapHa@T&P;#Bc@3mX8?KH1^d|ktE>YhH5ga_?Y+IfQrz8-*(lTFt&pZ|L;M%L3kBBNzosW2_IzQ6g8yO0Ob%{+j*CnD@1CBWre0I>Z{e>XSB~_)8B|oJles*p9NaAUUYucS?i7T?Y(mJHLqjgTwUD9eSCVvvosTo&XX{yP)d!KG$A2$z`deo90S|ui23>_e}&O!gZrMo||Y7KFuTE*^<Siu6yB`O%|lToL54YD)hO|yRQa5FD@c)8nrhvZzp7U|C^4)mu>>$cb)n<a_t5>hRbzl2O;j1l%g^Ue6ac?gRE4E{lJ{K7t=F2{IK*9E~tO8aZS7gCh?c+KyveIlmO7zDk}J-qFj$9CHTWa;iis`7MP1!j+{9n$J?9kZ|@I$)<bIACM`z^;G@DTTSTzHT^jFOiDPt6?8?{HkJ|!*~oSw&SMGA1gte@)g&Zls-9kp+A80eyIKdZ5W5ty`&f86z=u3e(*`oCMVl}9>y(FAMvsh<dp8CTujj_%|n)EJgKlfcd6azUp)B@Jd1PIKI#&W1}W>W7;o{=7EkD3HyNP8lZ(TX-_{;kX^&8ZC*R|dTRe4#r-Z>;lg+^zws0T99Auvv{c1rPtEeAPrDBKY^dIY+pe9m&72B26*tM2+q~gbq{vTsVp%_Ji`a%M`|9bLB)DDjP{wlmhvP`g@()}B2`;`bG5;PZAa+VohX>H*$R+i073d7<+<+yI~D=q<(L%L@I5UWY5MoqxhEw;Y~f6pYR#e$3$;^@K#kMYEz4K8jS?Ul)B2HG-7HNK*fnkxzrJ(K)q)k{w7N8k02)#EuvxW$%bh&99mw4FxV%xdNkPo*i7j{q(%{kN^-3z5h|V_5PM3Ev~(TO|8BDTHW@@gb90V?foOqCV{4&9|RE{A{5y8P<5B{f%F7j`5Ve#(<E9ct+#V1y5mic;N2wM_99Js+a-VZ=|QA?nbYQnRUnH?`t^r>VztEiUSook{A!}>k_AXk{QZ@$vZRUZt@M3-U`+Na4K3x`R5}-;vaxADCvrjEz2~7psve-h7@JnH`jh+9I+=h5qrw&BZnMo5&Uh1^8II_hk3A!$S&#AD+2sU^Xt23Mi`Fe|48>&R|zL9bPdZ!i`eTC(W~<$G5g(4{iDsP(lLHbBNh4)uT|(H<y~x!;b6N@<^MM}&o1fH6Z-GsQQw)`KH9+n;}>GS#e?|{PnYH$9*$2KlR06mw#Trycj=p%oy?;RYsS+)F7S+cM2+yUo0@pV<Ej<c*I3Q*Eng{m>A@bS+Z!!khbZM2NM-*f)=MF(CzX%rGE}5&!^??Oe)33&JgPC*ie#pl^mA5R?zz62#mLQD^mjeGUb^!a<|z045lf6GKS&Xlu&}iqtGTchTX1Ckp>|B+%>mMVV5fH%Gucws{tL%k#1PLAM|h60iD{=jaR$tkc{D@dTxk@2!7fWCf1JeavhKg>h$&;kGsKbMIiy>=OcaL~L?*l-_SaTYdDSj!T38!RnjD^UWCR6xPBE|Xj2HMV9u$tBas<s&j(ThJa<@}&HS@b;baic=t%Ehj)6H!P@tBEjf%b&MU7Bnj?$WMs+d9{a^YqP(dsgzx&~pDTS>Zv9g42&Uhj@Bx{MOpg%{wWYcTyUvHsQgduNq@j5sydh<beJQ$56J&7@y+67@srSw%UVnTuOKylW89Qs5(7@@@kT!AiJlD{!u+BbOYKZQ0T@+UqKD2?yL~T-XS6xE5vr{7D9{AUp3N&#UHccR-uhw*h9}0<LP1u@pS1f^g@kIbV;G>tYR-Sj63E2kW*Qt5=(WD6nunH(W=j{c=<>%r1apSJ(t*8XED~gnT7a;i|!i<rN?^&+;?Q1gDW5HJxIQ9u78vzu4sYWS6pITbsy1VObh4LXxLi=tS&VI&t^w+>1!#+pL?+N6xL#nS^H>bNB7@*_j<~=ucwF9JSeVVjSc5(T4O8t)kN8jI&5JFy*k%FL2@RleY9)#`Y+_eQA`mYwwstIJiK50J>vA(NF|x0)}GgQc=Xl1K9a;wpptZs>zVOnx2<x*GsHQ-bBy}mmL@=juzV9rHHU3HF;94!Bl%|XOaOd)RQo6k!|m62Pk6*B*A<T(Q3!1B|JaXge9Zmt==9C<PpVttZ_DKm9XatwYgp88Mra@H1oHla>%B77TV<*i^*T{48YlB5+cF@q3ABVFubwq53?7f`<`};)F5dizONa;3qcR}5DFZUk@&#Ru9lwwT5gfy+43jFlWTH3b*>yl!CJ&PLkmB+Uu#O`DTL(Lh7W-c^$=P>(%enII$|N6`a@f4`5ij3+exy62lawXWlO~%dD<#!r>z+mPtFe72AqzY;!V_eMK4K2>n0|yc9xX8*oy)~iadU8zCaUi)GD^PCrH^)8;Gsi3G{QqpxvqHF_Ps3eob63ZJc3qxl3gvFcr!>TxA+ixj;TKG$rPa#AvYuN{E-q>tNw`B)v80P+iNOu32jbq?U`&&VmtUZG1G1ToKSN)kxNgq4DEY@NK0O<!>X$@rgnJdj7Z%I^eZBL5>zCPm-jT;+}NA^;Ic&h8T!TSYbW7vt&(_&C$7W%h&9H;+qa&SLpJk!YC|LLd_k4l#)8Wq`C(B<9|S}mkAHF=yHFttNk3u<@Ekq$3AYwHEGBERxtJ`uO>;?#<X7FmQfWb_+BKn&7mn^sr^YeGN4k00xT$tdqq+afrj7wXQ&SUR^z38X=j-<$GIr`dfmO%D_6)0@!)7cjJ0r=!0$9(y8&ac(%O0?HKSd!$lg+PWka7}>t6UqN?)nQ0XuG9yJ^t@8;qL)0?U$jY)jqm6#eAgZG4>o!ao{-)>8?^JX~EbvD#^M=!eO4V#Ut0XV!zK^Yr7Q1lN$573@LQ=z7we-4lcMZt=m|v_n?F(s!RnzvpZ07(~S^X>04Akf9MO)#v%_3%@v{@zng9j<0)Pq#$!unZvxsv3n#p#B8F2*WZ{ey;4g^)`jMOp{uy?x{)~3Xf5jRSfd^_-b9~d)Y;L+L_TA&0yAhcr((A_6r>>-;dS9u36px7AK&}Z9yJ+r;h@Z^mOclbUP?OC`p~geqnJW7G5LU7HtZV<MD<+tttj9+TA(ID9^X+G1%mkBRGUB9}3Dh^;>*gr>L|Olc9*-XyFyYB=iq{oSwnMy@mp-J>&@|b+k`DTaCDYT>{_U?W_EA>97rcFRX832RWo&dx9DNKa?1Qr!cK4+7f%9^n7P@n-{J4u`DYqcDwChHiL!Td}q_xk7b*$Q@37%D-q7=3Le8d>As!tZMr5Y{|Z6caksf*PrKJ5o*Ojv634wClbzNf4ht7pYoR;+H1UA;Ya!`I&B9V=tV%HCw)wCZ*ZB-F%u{&{SjdxU4P4M1}Nf9%F^g&?Z^h-HnZOL1Fr)$e+|x$Ds=lU<fDZRx!aMWi|FOh8SQ`bzqzxJ0tQ_eB;~s)Z92mJ9!g*J18Qy3JyjaI{yG%{8CRaR$1HMP3*zsJe0-)!QXKyx|;0Y9H}3QahyG!f)wd9DgXL`M9YqyGo)jbZBZF>px~BedB;f!bjpqA|&j_mS2(Z(dDp9s%cnWp86VyFe3AVUqt2A&O5E((`#DMd5`0_Mmui4;u3&5Ui7#3e+gIIw9vKAaMXoiT3<L#EtX?<n%YOZS`3a|hPIw_A~M7h5ILl~E;68SN+-<g(}9ZX04J#U0ub~~KkXy0ft08)?<0m7NO>?iDQtEIn)|=O4&wl#H*km_P2WGV$5C(U?>=G*a6HQ*W(_M9A&>D`mq}8?F}xya9^I?Sutvg%=q(sUT;jnf9Q}wP#<On`0|__B_h!+^@`9ipGYOfm39#Ej#ok&~drPxYb5b65W)`e|0d4>2Eiw4wx+}O-99&uOXmUyWyBJpCH`!c;4+<_&m0Vt(npBwmexLr)9=mdk*_6ly{1LBRz>gGn$F7$6*wwlA4g~Dno2E@lZ$1PS9xD_Lo>8^r@sy}s>Lbn}o}L-oX^+O^x0-C8<^sxKwI^5ly%Sh6`T>dMe9|>c*fo4lOxO+ozcLxpjc)@remZP2Gl$}I?xt}r>D^}D7SRKEJlW-&$5P91lGI0x0iOJ1pa4_}W6&m>W6<6J<?!JB#O);Q5-QZ(Z9#atCadJi3r1DyiV?w$)j7nW)sZI!FwF-op$g@Z54&9U>W)>Pqxfd@P-IbE)}Oe+<I%&e<_OOaOMpjhzHEJG#l1gGHqWGhc1t^}bM0t(1<zt<Pyfg(JT`hVcu<1C{71Tb(P_W<#u>u-eZG%Cv3HhC18wh8hQOs6@NVe=$J^{PS_{vy{bwXks~iJsC8AZ_xZ5kOa)hFFd&6UV)@m}elHIooIbW>xeS)<ana@9;4m&BjD0XchOZK#?9aulps&@VCM623m3c)~IiRam}iBl<FwE?JDgDpsZb_QFd>P=Ju7mpm9FH_h`{}Bf!>XdFOJX=T_R^0=es(WY=hrQ)U-#~(kOReDIQXBc>J0yU2NFMb20&7e}O+a7m9;n6$E^j{fJ2z7AsVdD;Zl2iF{;~Gg`wFZ6aS1C<J^zR!58#yUoJhKv8(rq8g8`iO;0tT9OU3H*L6y+^jlC5$oO1FJmw*V`Uh^dptl<zC=<imu?Me3S0x%2zJ3fT<eOk^3=pU_%bgY?6VJ7+`UfSu8wCrd>7}X=pug^FtPp+(b3=$l+KT@TNsk#WQ;#kUN;AFe>X@)bz5u9vWH`j1PVpWoP6f&GuCzC4Sbrdo`yz)mc#v@upUX{=<BB{r6jBt^}N1Ov%Vv8Twv@p)Zwir7aF9dz!w9~71TkR@AaWygRV_oQ#9${OkS3?}>mD<v?b*1N6f3U9f<TA%4y~4v5eLyNf*NfFFZU3{1wEL}!J}mUC?Fx0CWdB$ktaHUk1nU$Bu+Hfy+oyWL_$^wJeTy#cEgG;s+ar-BBPi8&D=@>;O%gqU;NFMYM>jXQ9;K}%Y8m>7IlwiF__k$w9KM)Us;0C7UCFg(d2_B;&5QMs?W6OSj^Br2UBgE#Asl?rFjNb~H8NV~Bz7}Au7(ln@3R0^vLCeFKH8#1Oh*%0lxO>jS9!KmTJ}9=!eOPW@cjNP*?N#YiB_I@OUD$adj46S&Zn#jegI=tObb|a45-KvtRa>F7TFbw>uwm06)4NBKuMA<9pdu)hmUqO@`4E4{-FM&I8M`k#gY2(lY#$@NcbS#<Eq&_=VI5erp(tC#TTynB7WF(i}LIi8yp+Mq5_7mxCD5_RzIvOJt*!{xVCGUN{@C%tZ5eFhutu$l1ZpVJ6!w7i-+K9U4X0AKI`}2UJf@W)T<$m^s3nE{h+Z5<G!kyRk>M{@Ka&*JEiJ(fw=O;{?&$U{RdWScV{|79H~`(vMsym$4%Rrm|}cY``p-d@734X-k{{SwDhlb*fNgAbWzjGS4?X_UAli<OFGJfgOa?;|CW+G0e*JyX#OKVWe4CJVvny2b`TG5_=*GNI;C}6&KOt6>KOBVWmnMkQz5>fQ2>7bZ~KY?l^cU}iZP}l589}b#BEfGjYc6mD*4^VBuKqv)&pw3m9<H5j9-d6^1kBLbJ8iz+c*AHAnwL5V7l=Oc|c-E5dRhsp`U`<zh?4)s3#W@B0xio0U$aO+p_KTpc?mjz`W!SKz*+V>a$3eYg5QvdQnQdx1K(@wf}v^m$LQ$)wlq5(>wGPLja8JT+DTzWQ_V>=Sfgao`ltf{@0qC7m8}y-+@J}-@n>oKRh-+V!qL+A&xZa*x8<T4QHCekyh=0(mc{CIXsFP-W;9+JsPA19<WtM{TJ4F1?g8@0z8LwZ?cJFlTBz$wq4lTe28mvetClrrp1*eh<V8;8e;Vo=a>)ed-MJ+)zy++18{l!a7kLYK_Z1$bi!Q}UvUlTP<k-DAl}!}@f6EP@0z9S!yVg?eMU{NFx9>~0>{45s8R1L)-|jlg}o)h8d{>9s<A}b`;3?1_O_aqIpti>KNCLYHRc6aUL||ebS}z_Oq1WEIq||Hdf85$q1eanYl!E;Ja*4XyQph<Gi%aGl$AbJN=rU9O0<2%67m6hz-RP;ulbC}`U{_&s~hbk`0$D3@<$$H=N<toi{T;tYlRiC=I$hnfpsy3zz`oydV+9zNR!ReLlUEFB%(?=c&q$FjcR8UJfM26{=NXh!+}oGW%m&;FFQQF@9o0~ay*O%S)vWViEQC*>e1bsk0c$+%5SRKF0eR%t?8MusLHHr2GxV?5)}&lCMu@ynq8Kg=+=Pe`d#N$fgg?{hQp6^kowo+A?5g??JMR0(va?MycTZ6bHmi+1Gu`rMn-!0h?`4QrtxQy$GEDla8<jys{dkf8Mg_SxCVhMu0y&VsM2d2LR?wHDKssV&9P%elFOS@XrPxK_@qkvTGV4Jw#0A7R$OoG726@bzX#LbRt@HZA0k@6A9UIHoBu)8Ke4=8NC|*Y7pU;uL~Bt&wD$BX#t@Y`u1i!h`W=_7q^N|fn&Jc+*8iQ?2l4ytZ~gv*)6*ej4H#8eU?e*aagZ?m=oTZTJB*a}7?l!)Du)F^jPquZj!xy(DjonrzmK_nB|BLQaS^vns7|r0QFSS7e6IEbTS%c1s=mh~LRIoS@c5t?j^Fr0Ry+#S3@a9w{G_vyKXAW3;-&lbBfX#3)_!13flqPBu0BQoV5YBkW@-cM-vY|C@+UIcS&98O-qm5yZ5;;v%=+CaQ?FAB0l_tb?jOeF+Zhl<>GQknaAtiBtgH-G9y59jFmn3}CtyQd17M>TW2;faA6x^=RcByT?2#+k-U;D@dg;?Zh>I$X!En!bjghv7myD8Q&#T^?z#+~dFVjjE@w&k^(<;_`C<R{~!vzP=1sz)1CyuxCe#FZ{`mZ!^ZF%t4aLBH24gG^ze{^_b=?$YKiz$e`VjQZcl8<8&>dX*FIx{Mu>(&{_p--hmGJ61XR_}1Tgr6O;YwabCCBTn(Y4%;<{ppO^^Aa~qbK*6`9PrYkSao;CO3JZEFip%f9s!c`2r@n#$d#MJnTlXfxJZ8NcWPNYAGfb|xzG6ZAoF9@gl>o_L<eOb>GV<lq5G~eI&mI*^AaC|>k|n3Dzj$q#((=Ga3!hF{I#!T2@cE6JvhWQ1cwjep3HHKnEASKtACJrmv8&TW}w+0TCR3z7w%NaX>r>fledEhiH}$VD#f}MYbs*}`kG3CSe=q{Ju~XV&duIZj@$>r7A#|+^)q%Y$Xu`!EK7Fz#)=IwhwMzt*_vH0q1De>!bm!6!Z?!7Ncq(Q7-#|i!uXdl2+?4x@j8mHxCGehewoSf^FwIY{Coe`bsi_vPhnMgH@@kKGf$_5611X&`Nm9Dx4V6&(;r)R4wj6GNNf8pnZpBF#5$vru0zt^NM*IZLyOSn=kMFZs^<ncHeRPVFkVBtH|Z&6l~bOB<qTpoFRb-pXFm2sMeY@jlmQ;QT|IbO@Ny&0@z~QEzZQPP67YiKwQzUxgn~xBL&o#r>!aYUY3lixHxD1C6`=mrDir^WQCyp*k9cXCKGK~ZPk>=`LlA7i?J^YW)1xMQHB;57EaGH5JDEHy2zKUt#LJyIr!?;~RU|29mCGCnlClLv({}q8Lgk<BU<MGlou%h*EceP(Zk4HQ%Bs3`0S>Jb1BRB|zB(WHB)zLlfWCvcw=&yxxX3?sttnzP#1ODTJ5+3~OkBdO3$d8fCTJ}aO+h2Q0jTogw6(7bWun9SVgzW2DF8(KLXX4rN_^GzGM~^-cQ0I7Fu$uzxR`9v``}#(!_SV^_}~q3bntjzuQwh8`Q|l{%@ZKmK&Blweb+J}p{@WCF9Gq5G7T}sfOv1Z3&ns689;@r15~lRJ~OQG5Lq0#z5#Sa3GXT4Eu}8qCiS8rm`B#B7&TIt3)b|)BFa84Ag`!YT^z1_^FZ|x=YUE+3{l!r37=n7=Enpzi@c6xD8Ipk5EeH*A=MR=xCHhit|1eAaMRb=Ypk;$)Gn$VhyG`v9tI(t+o?)WTBz3>exK4}@B_m79AZ}k%SSHl&OEDoue#FGM-Nh>as7d^Nm^bLB`$+`y~pk|n7x-xTlB+yNXpX;CT66Cn&r_hW589r=4A#q#F0T7Qr$U}iedQ1n&$A0i!<9iCd~i#@a0t&u76cWWo%hk&%yeNWliN=(Xe;3Q<&G~7D>i#c5-fZdECMX(koBKpRqo^;&NFu>$R`7uGsa*OpV!faS7SclXzDpJc})43p1Ehiu1s!>bqX0@S@KFC1)S+_PG`%K7FS+IDK%0DBENc#n0#j^XtJ`**T{J1cKCwEjuU*TF0OBnA7|WS6TF2wXbD?3*G3#3D*#p5SO`7gY-aXt4PFX6Ifi$Uxs%xO;T4bgCgq+52fhs;3HnX9ps)Izz5}H41lrKGF}5SlFOcGc{fIBuGzxRRB}(m<2P~y=p()X#QQ=6F7aJgX8p;@;bG3{WjteEp2bt06E(J$Yh;Z+VhZpK=@w7QF`x0IcF`Zu6uVKRP@h#F&=CywL1d}V^?&SumN9C<uq)zbW*iH<<1Ft-Tmp7%mpTQvUYxj811?$3!mM|QN`j`jEr{-Q#896SYBq3@j;muCm?4yp0X+~A8e$9x9pTRQp`bf9@ny%jGvDyW{ohkF3ns-iGUQ_hjYfg*N1OvD@~H5F(~ZGSVTpMQzU;FT4FmQ|K9oiY=FF#k<%LDkn{@X_9GxoNcf)+0p0&=PG1){ugPZx)$I3pPjuw3K%Ic^cYi0R#v4(u;L1iy-TqD-5Uc*ORRuU`ZXCCDBt)Q2%J_eSHu7J5#y?l-ekAV$w1niLF)(EAzyrSuv0fy`>wLpGZqa?3RoaDL$6wU4biX{dl`+NXtxw**1Sn29wl4dFDt4=m0_1i1^S4t~D;i%QGIER1$9++&56LUN*Hs~M7KT(!z(Dp*x6!q2&59r)hW&qV=T1x0-4{_vV*W<?9+oT%}Vyi);`*IG`(nHs=d&)omq_J}388G&{Xxi5-vIJId9HZ(sThG9-?w^*3DGqBHiUkJ#o$}qVt)r+bK&7kz#ntm4aSj1JsfnY^vC+<GZ#nT*0rlOtH^RKqAk3T{AUHzP;H}^zu4_PDnjhpv;6{VMY7qK7lD^<uQ&SM14WZ7;L&2kMBC<oLcxi{^*o><Cr?xxSG0jf`eQ&{{)aKoLjARjxwV2M;1j;!REbxRrVh!-%@u0m+7Ga9RF-#^LLvW?bAeA2WJYA=TjBVzRfMwx@>zKvmYUL*c#RyoAvm+nr&NBk0P@!O~P5T0a%=YOP;Hpt?SfQ+ox9$EldqCo9ASOUVTmwLdbQ|Eq=z3fOI_s&wfYiO?_7x-VdPb5KeNOFbPAfpY3OUDsx>!O$Q1@o~To155as=kcv1?Stl;zceBobU5vHk|?0qGHbg-1;7Sg;9@+R$zdD1M7XbIl{UHn3*V(z9R|sP+z==8QIPf#ET&y6D<zUtOpsKfy$J-ioWGywWOm(@*dk0y%bzYWPUm{uetJl8U|?)+(&R+V*wH3OVwzKVl47nP-E*g}ez$vJFav=_a*JRo-T`n0wK}w**U8`NocyxLZJeCL3;30CJO55}Q=g>{0-uaq-twa8cjZzB+eu{4l+zqPJ9@q$0~r`nuTkb#?ui?L^+z;pq1t#E(N+UB>>(x=@`lwvZx7e(2#9Qhio8f$63^*_5|6e~R6+5z@O}x2moHjVdw+2*W<5kGO<@%y^JBpnUTLnWcmoe+71xH;~@+2AG@?8cS&}l+t)=l|YTY;*}Z=DZzH2TG&;2h^84uIVUq;Jr7s>DGQFeY*rZ}_B7~dq0xxO5SM@k9n-Hjqj+<MaQ%0aQJ#5F%@#<8yAfB-cr{02{F(i3kToD&OjE^C3T6OhJ8NkMG{hPJ%16$ccIpyN1IzR1>&$jVQSU$gwYK2c?mNRMd2x0|m=(ehSI2&ZA^V5u{)0Hy7Gk3<G{W&K5R_}HDE%HzEv~YU+@!|zc~C7e9MHAPJfuC1mUo)Mv0<4P?ljF&t14RGK1Kw7XLI|;D>zthHOF3E22S0M9?Tq{A~MIwouc0#B3e}D5!=}tX*?a{jh+l~q$g_ET&_)SrqJTf)0wk*!02@9?@tYbUkJlsUzv|}?nh}xif9y9*m0#%>|Uld4T@=?p<yR^HyY^=XH^Ge#%(r}$$6nC)iy!RWQI68GVY;lePlE~GuOcd^QyeXHN@Bb3;(9MrV*v;P7{kAuXX;=5_ENb#LKSEA>Gy`9!`qH{tHHnm~v(diTMvZ7T1t6_;tb=>?o0=!zG?+`V|L0?>X*{UNkvkW^pzzn4=L<HqN|iWvLHARdVidu6BcGIv#GqW$<5daOU9Au9J3n;xkuFY+yO@^=bKj!7vxp++zOJM|YNIoMf@7N(gAhDV&h_6-&seZ}7ma)s9ac-JH4%&VoZUAO4Syosh5G6yi0d=+WXUt}!Lr?t{(e=D;;3$tJim&1Zx1A5K;Y;EYbD8IC6F5gxCg-OpKH{2%f1;vZ7l&RNd!I{5SEyIg72$Kk(s3XA&P!KI|)Y4o@9U`*Xdyc|<^N^$QLMy;D&G_er0H`nm3AMGxy`5BrjJ_4_T>d{F2Hu@D)fJST+%o>eAh$ia{M=cydOwTrS^d*)^&FAD}&fHT{W4o-`N?8|2O5^1dUvUXYJ&P=)#!58b@@nmH`@#R7mwYqwx>(bZ>`c1xTf^~(UvUmW^*4&0?a^>yR}<y%`ck{EBkOeM|K39Xtj6-2x;&>6_uYTR(a|fmvwF=*B)q`XO=Y-%D#d}qS7Q)J1r|&4&Mk&BsCvv8fC$tOV*sii!`?T`%-h`^p1j#@9BW%AGOFp#oLDafS&SM@$SX2^!-m9UAO{}xN4m2~HQXQ6M6(A}M+WT*r<vsZW>nTW$N~;g1gGA@32*%&)g7ETWmrwhu!?7s-@f<zkUd__mh-2o&-bDzw*L`hfTv4kpERrtF&|Q+J-K>9e>(n)qmfpUSJS^}g$JUJ!>^b_JdchR->FYBcx+avT<ccr3s)DXMXNXu7vTy=kGH94Yw1_KjJ<xOyS$K$vWFELfmw58Oq;l{5uk9vq^<V3Olla<R*z6+?==E@2U~yo2Xlmq!+`{+6!xgjGK(cEQkdcop<<V{w2&L+&x#KtJhCV}U~xuz^$Q)Ipef@c#k9qP6ylLWGoG5s;u*KgFPY>;pwT~yXL7U$#ws(!k;;f|yjwC6NTJHe)aukfkcl>=KV!UVm=*jhY5Pcv19h|G{wv0SPPLDyOFA4slM0(>Qeo9>H6~y0EqMcvuJE`pRqvf;y>*uLbe4BdKEfq@^Jz3}<@beDwKu#lu|6D#ntJQ|`Tyi8x0NB%QENYQ>+9<wzS2%{aHVzWK4T_lLGAj`Vbb}~joV~8b?wl;YNy+s=kXNI3p|IvgDJuTZcFzS&zSECJGGK<t8|lPoOP*Gw7QgTi|GE$p)RZCc16~ROaY}u)Ws!4gt~p+XxCM(@l}?qxyp>oC|KVpf2u26-9CC--P7zG+v-ys7^724+e4)gA1azHkYt;qmz<@%d6i)=k#_|@eU7ui)AKt=S>W2{e8fweQ;zbd{X>tysOMh$(cPj1wi-!nim}Rd$=D0UKkXb<PpY^ImvU}J336p0F@>be;Ho7l)bK&0$*9bDs(sQBgcsg45GiM0J2#494HLPsHy?2cn1~(7v}S_gCh{f&qcV1Rv;bdg<@I(3RnPM`?*X}DW`0Ubl6qRB^}*26cv9t8oC8{ibhk>gU*R0m>SHp(vqsiZ1D5h;x)qytlP9IBD^hmLharU!>(wFMTBSxKbg*G~i5EP`4i6`I(-T5spEv7kF{;Ni0*o5rImGL{SveeR*c5^vKQFXIFfXue-bt~iE@i!YP|SoNeLT9<1nFr>FQYF;>>vF}>jf{-9)_=2*MNp}2PpFxMjBN0s;Trqbk&+r)2o+0t6z83J}!<Iz^x$yxHCim`!N*I*C&P1eNH7@sofV=m`oa%tQb6yxj|ID0;{UF^9xv>f-Am}r+=(Va1>S3f5asQN83~pSAr-6Y4?)Ok7UD!U17-|&k~5}RR2hJ+>lE;HHu$9;u_Pb``1Ja;bcrDENDbQ`(~B|;MMDm>%xAsuR<$7GZk*s$WCOKLE!`Qbd!+HHX)lC9G<@Y^HuXCSzVnyin?c^dM-r`JnfHo>1o%aqXoB*tsG7^9j8nKMQTXEINi<cUI*yjZMWBUUTZUWy-QjiJD5_`+x7O5JUBWGcL<JbkDLJq`&wBZ<LZ~(Oz__>NE`h#k3!Ags?yjcWQf$XCK8S8`ieCsvd`_ajw-w*5*3&nNiOc8h$Yb*Ek5EmdbN*|JQH?hi4zlc1^ZVfZ0q%x)(14c{XuSS|7zW&RN>*8(9f*%-XbLUkXuO$r?MnFK=?>Y_XZ!Nh+95hJtA&-KToj#e&m=NrrBykCR?hO#z@Uy>QaG4NL#>8fIoj$G=AzU#*hg<Xc&VVBs^d9FbYv8B8o4Zt~0eb6RDt!%S&(SVLsx>;;0?zxh66yOxH*>Xwp5Lr{cY!Lhp9dE%q*8MTHe9Op&|z5!a9u(w@{9QLqM8=ekcs*Utf{Z|DyB#^v_W&fq?OR5Y>lD=smSvOi52%_u))IESqbi3Ect4mBirqx}*V3R6}WS8-*V6Q1JcnO^Y}+xHzDEe!*V_IoirTB&;NN%Cf0XM<8!q+D@KJqJnv-H_6ta_A%7wYeb1mBQy)y5MnDc~J1qRc@@M&zY!H^jyFGq(?KMFU@Noxp+kN=xq;u<spuI<uPe;>jjl~P5_KW=2zFpl~+N|_-5rIrJ8Wj>x|xU?oDAidKdbL1LNGK^59}8im$P;UG0L0UFRkwNN=V(`g(!lyPb>Oe%F5?-Xs`5;^3C=(*46nPhj2nf*kwM47Wb?T>`Bgl@_6%e_HH|55V=U>JA)@X<`>}M~y-sII+I;zv7i%4Jp9(Iks1RNf@KzJaK`A6kR}u<w?H>{H;<8=F9lAl*~^%$!n>;m3*kM!6mVteZ?B!I;6W}NR^@+kLv6XP#v`_7g$v2h2o+<xy(H|wvXZ!XjH+hbJt&SUK8ok#%rTMz0*H3OihN7e@&ci-!$Qr9Ku)n?yGOSr(zywdv){?UIn`I5ifP;N4k4NLxHfa5hazV3=72?bIhsWvnHzsISWxz?vv5vDL3&Gaoa}{&rMtt@Wf4AmDZKpA;lfHb3X5q8*-VVT0DnvTxq4Ly6^5)TSZm-AargnD(X{Qgo`N#gw|8&zqoW?C|0u}j?}E!B^4`NK)FOUM@=?&pF?&Qy{Xpk-EZbaTQK+SgbvBM;w{pkQygeem)33Je&3`V*(hu#IkHjhY8PXpum_`W<^Z_4?S8m}e~BDFwoksxF)7q_o$-*;-W}+*7$GUl^FuA7tLa0~>paEVp8VQ3Pn29+5GiBQ$fHphm_4pcc4Wx$@Orr2(OEmi!C4!_4R$q1NGZ&>HCdA2Wdy0%+#4pnKHR>bSm*vFLyGMrtMkW75WjrIH72D`j$r5}V0RPdbB8v(L+Z}ci}4HhS~@>eC1;bj?LQCW&a01jSwV72_t7t=Xme+mNtyAa!uH%HDwlrZ{05%IIcpzviARI9_E(Izcxa0!^t<~^4>Wjkad`6E+9NCN5sL8SdpvTBr|$5SFo^5QA~T*^A<R?usno9)vT^sR9I2jn_oRGp)kMlKe!G$yo88imRQy2FgC0W)#V8Wg7n0ol*ON!0b|U5XSK%#^W#a9W?w@4auSAHE%o94>Uu-QiywWklg_C<)Y#(89pmJRA_!XCc$sye{0f^OPOjfus0bB3b{u=x}lbjYiIC4T=*x)htIJCjVZNI%T89hc@CaK0(RLbUx0z}UwzgZ-c6Z^4x{bTibj`47@Wf@`(@c?bd+%~hCImA<8O7SCr3sV1WEB`_yvd|coyhOtHNca}XzN`u%nvZ<QI1IZ$)gGumPT|e>p*~}6p*$JZq@vZ4UvZ8}mA&%x0^gX_{P>Dcm>nLtdmy5WEwXFzg|HS<dOA^WG{KmWdQAT6hGUCQs7<Fh(554`@!;w%aT+LTiC!Bqc^9XYn>@8}i&eD6vC@=IAwPvB{<$cFlCEIc+D-!t>bj<ANKv-EdF?mGaeQJE$EU15p2%{;2%8_INSwHZ<0Gy4gb^aUtWbad^oP#x5T6-gxCG!M-Ct-W9Oux>W3$mB_IgD0>I6&7eyLRdXw$BAjC|8bg?_|q75Yed7qMeFOs~o2VS32frGmP4yo*PD_iOuTrxc7|i1`)|<~uxHns<0OK4Hx<F$oQ|eT=<tOyA7+WFF0vGoJQwfoBvoYJ`W~q{b^ASMs>N(Q1zG{t6|^jP>Z<UT*nXVw_(fmHh)+?(^6@seHtop(14)bWWu5lP5*wQCziFB$x;8^>bESM!LS5+sMs_^w&PSCcX0)<|rfm5lf6GKS&&wusXIKt9kMTwvfrXM(vown<K0HkWlYAX0k=F{TGgDj3J&Oj_@4A8q-dD;=CBmeGAPHIIp_<gue$Onf!4Qw?(`ErX$jf4bKoqhUbuO?J`jubP&Zs2eH4wTBQG~iPp3*Jeo8)Qs;yU3P_z|UXvOx_ghjZ9KZEQGH*TV4bRKnQ@wM|Z>`bQ4R$sq))-H>yeY(ECdmbg6b_YXvU#XXyUlGUUL(f(W=g(K%JlwE|1VkLL5xDyk2r^Tdi(v>@X*bh>PVbk@C;R(@L<tb?Xr%`z~E6kouL21F$^xU%cnT7%jc}Pt@a?^Smrod9re$%9$meq7g8Su*}Yx#kLp388w@vrLN~Sg3TjAoXN53!6q|`LR*3DiF9gw7i}Kq+7Jtl&Tf#PeVUI#njHins#M7m_C=4|=(Gm^LZT~K@ml?*LcYnyKEOL#dx>pQ7BC2TH=vTaa#TZh0@DiU(Y^^gJYu(Fy!Y44gZ%ia;rjHdNQG$d|mo6mVhSxvJl2)|2?kg@at-6oxF@}Za#izFySXFAV@3Yy}seb>_j9z&PYcW2peYC@?`|rJbUFF-?)kA6?9M`bMlJ&qs6)_gdcI7o22K4Gyfu?rOLbZ=}zhD1_eE5ke!ozkK^n{1^yT?bw9^0tIbNt$Zd6(M1`T;LE0+PhfyON~2cs$vy$(-;EaSretiw3r(3s51f`h>GF`GN->p5{lsnMM-;-(uB1%EE5@MdA}4amsbYBS#Pd+v`7eBir3>u75`dQ^sfI=RKm4+Tv0K?NghcE`d?MnW25OW6%2!uJ_7RZ<VQ9)a#VB=uMdr*)k!pskGFbhF8xT76y;Ub%Bgu7}t4z#3jT7=~0=G+>{BKXZwP#29ICJhX__#s7w`IGSM6J>^i3`lLu-1=Ay*q9o1a+0Brs1G&=Tw$s}jrIWFhAy(^P^T*_h7%SXI?`T3FVj80OPNKaB9v~*>XRFf@{7R|4Q_ML<*@YD!TkQn-iImBby7}j{S#CUWrCribR)k%6x?s#*JQu2i`eURh=4;|{E5gu~Nb;ZNBU1o_#FxDPHt3Ao?G){cmRi-slGayizbj);oM2J>|+!(_1M@m%9`XgRfvks|luc^c(v^o9pIT~V_*v=bH%yip=m#I%5q^7X7g%}STHrgec7kjzt>bR+$+Bwrww}$;nOP|CSiQ_dwy>w@)97=vTS_?xfznO>aB>V+d5-;(@C7U0y#&~%90F-jbX1>t-C=++opvvtO%Vt>W_;b^whdl^=#t?Zv{vmqoWQ8a^{fH&tbM)vZ+*<3f@GSilm0qm1Jz-KLzv>c}N((yGt}BJSaC&DtHI6Sn(#<2sO=WZwO;69Mj@d#}FH{TtgXE$eciu0loH!SST<XJ$7^&mAdxlldOT@ym<ChF9fb}@NAvJoqY#nQNRTN${S@?DaDJQYy%C%4Gu0>IDcQI3+x6(J7JUoP&z?VC;kM7km9~pa$J<3xYc$7oBt0n67XE!cWu`ZKvn0`zO*BxiSPiSi@-nl&XsCiQ8>Vzj!Lmb?8U0Sy>TJK#6%~hci4c%?$<o~bjY?MStQ3d=)TLKFBF!%jm<7!N&qd?-G&dyBrR>_>*&DV;6{1l9#H7oR%BO)I_8*@cgXeL$d_-=YQn5Wo1n8)nSUOe;)Eo>y$xYU`GCR*W)(BL150NT!<ZPDC%V<2fX-~LxDAraWvMzzGZUM<V5S9yI7Is14-!J-QK`;XGDPC~JUQ(t6@D4S+q6QXRqygQ=2)BZD62!lmUHV2D(xwAP_Dt~uv1=U7l`=WSEFh$XjOAH~CjT-#+nfSm&7be4hnwdcT($;Q9qYa?-MYMQq4}u9#F&)D0c*@!!%ryWYg^K29%;E2?fA-kB*8;Ua{^^_{$zq3<w?(6de<p0kN=IW?WJuxNQ>$TBP&!BU-OiJ<ckT%x`9b~Y?_bTU-5rf9y$`K{)ox9&uzFjjs9xw2V*sn}41`NH9A(->^W9#>cy1N<3dx(UZk|^L5rpo=m0ew3GFA&K7xrL19=!T^@P_667LlwOLs+Y(UI%@om%)^&4{g%^{vquM^DZA}0D%+*d-6t$hQP_a#O(9wQhZIu>eoF!T=&b_lXX7BG&T6y9YHP4X&yf}y|_NL*WUZgj+Scf1ceF3KVskIy`-<vZ4Fz6HD6?I71j)Apa);%gQkV;WIBeUShXaFHC!T3?Gn45+9B;4{*?~K{)a&9f5@7cl~_I~@{~G;kj!-Yv;#{gAMqfL=w#C<{Z1zzB@u7IHVJdl(=;e<b<S)X85aJuVoxgAsGC$YhUECxXwuHFSOZYUqyMWZpy8mMGlp@4BgP|ZEdPG6np>C6!D@@^tTQ;KEUp=!6Oke2fXE?zD>(xSn}Gt&@A%F4FjeFP6+a+?wnV5cW}k>gxqFuwVj{ZHi770YZlcLUg|(pv5PgAzZ8!YBm@SU_kfL{qDZsH8T`UqNGeTjNxiMAc<<LM$@M&PIk+AN%g3aw1Ux<-wdWj*%^IlK}9FDXHo~)NS_R6@nu!NHD!LY|%M&fqU6fOPX@(3jDaKmax<Mu_XiNWUI-MO9O;L3un<|pl!80P6W8I=f}fN@{B=%+oHG-o?zs4uPuP>*Jw92tf`V&5>lr1*9KtHuYg&bJmYtX*{4z7qQKsYvq}@3`PO=Rp}f8YNd<;u7NN@v^s#BylsZCYyW1fMi&APpR~Gdr%A7-ivy^`YueeDHNZWWHWBy&SXem%p9ok<6@I7T`f*KZ)y&czAXaQQUENTVxrJv;AYsD>Jnpsr|k43fXbm;+GMj^+N-4u9(-=>-E(W}`BMw+GOF2ORJnSL2p+V~Ar7?;@1)B#b5z4ER4_*lS2GvP32O$`r=>8;PFh-j4+l$14i#G>QbWuEDOuxb{!JFw3^jE;zg7d28U=K(g!u%IGdrj+W)IKmQ!RLq1JC_O`u610zWBjHLJl_)k81qsqMsV*K1CPXvB|Z&6{!c$5^R%mOw0D)kSsYpTI6y>PECW|J2^d^)%tqzV|)b1poAN)w9bWvQgYhs>Y4f!r+WUZoAn+|yRk#7YO%EH+AV*kRo5}O6Ro<AatIpea@;=`7e|)jnIC|HH5kYAZ|AYOvQ}Lw7>DJU%bCK;{7W3Dt5f=tCfa<~Fm)gDlDZGgC0Q>&k}r^;bJr<4cio6IKOq5pLb6da3|V3#vUvP;9Z1y}!R>+Q(J+vLJwV^PZ0K&EYSm&hv-+aTDh^Zsh$Fk;l)jmk^w6eS%ghSEY##hzdbidwIb$YL39U8Rt69UIDVJCSBDj{TyF{>tEoA^z!t^Uj>w5=a;rzdRKI=2SjA_spSIg;`f|tUu^(A&y_9fLfau9|9mZ|4WS<)I<>Rw84?*3_8)!secz`-#nZh=$QK~yuGA&%gbwG`Bc!yIobo?9=&nXNMICA_y@&YQP?<E*>5i0ocMI}xQGQ#!(d7MHjLxOmN=^l@Pvw(Z60(JAaLuFb~!wsx-)s7a=2i`Cg!a(H&(#v0;iW67Gc=BJC66#6SE^K2^h)Q~k>1d;}2H0#FVOeneCRhq^O^Br34J~d`#UsOZuoOKe>I>iC4v)#(;6F=j_6ZV>mENY&x0qfovi(X`_+<29y*3zbRP)o9XjoPAV)Lf68SdD6sE^!WMjqJbI{JsovLWG15k#^`#QYrjp`><M^`Ic?bm{G^KVwf3mi8%y?8`VfvA<nZAnGLC?giInN)W1gxDi_=3yDhGnkC=`MwJ0q2EB3;2r&Qky*f@vjvP^TjESJ@^ObnuXwv)8|CX%62$s-A8hn0YB^H>Fw!-|gH7bRjf#2m1qb;P4zUL*1TDLn6=#A(ev%KG<bl6FgGhe%!vj`~OCIH>z8j&@Jp88*;}gfH1mMzf-B{@41rU-G^D@q=4m**3bas_QTv98=7q%!gmG26%W)oA6UaD6Vw4&-|PlJ0{_^fml0>d9%8v2ut%$j-Nc7!5wh5I^b$u(*gRo>tXSP`)Y`zeWhy!Kv1X^ZY!CiB}w#QMnh`4;_oWE-vw+kAp7d-!}Tw$tKOaQ3~{ut>drXt#^9ljjhBpmk@a=5ZNB{J<84F1c0^2HU3)s?7?u}R&isnW2h^qSZ*fURZzUg)O8#&1`WW)g$%FsgvLX9V++_z~OKgu-haJS7A%4Zd<~pVIH77Doux+<JF6<OdQ5NC{I|{%~C2p%2P<hZurx;@@yitplIIhJ?xa=s*hD!PN!3h$M0d)%?WLk9+9KD^Qvc6xj_vCa+%WFzNQix0a6_!$eB@2j2zkC6JL};6>_SHfb5cQ}dLIh}tF#tqIz?)wSMktKprlEIk1tTD~U<75)scoiq%6$;M<kTCuZ39!A1K0mwqXX=r0O?l@0kFLG-1K86V?MVJ443Dn9!Xo>wR;NA2RS(HH{kgFeRa(e#A5;_mWLfR#L<pAhRUb6sx>KLzpT{Dffl3Em_`M@?92k)8zcu0tVv4!3u`<K^;fI`o<sVsvME25&!$CXuaje&4RNpb*Dv^Ba;~(VPU!N9I%NHdOU#Gf3;h0@ylgI}idersT$~&;h*`5oCmeb4E3P3ON*mn};;|qdkFI>=sp7ioUPTSef9;}^1digiITScXpGLKRzhd#x4Jo|qC9JVt(i+x{aIwbZJ)!sMoj3J@lw5mRM#-7CFTJxJ!OBjHRa)o#&PZw5@tzYqJp7v14mT8A-<cNcJFkOQ+7({)%YaHJq2AuAlw3YFiosoC4*39W@W~W+xuH78;BqO|bd+M;d`Q6_oEkq$-OU_;t{EM(v*`fvfHgxXVGOK`DFlYN(O?R~MkP&_XFZnD6em%o5<F@C<BqD?=ApFJ@j`Y7gokOJqHFLHyVoF|cKPe`3Ff%(4d#2;ZPr$_X7Hx&_kH<%(xG@3RW0iTjq_`lGMp8a&RcCkwGnqBrO=`xnd@{?HUV&=M-ARi+4T<%Q)vg%VPP6beN`=_5<kcNigN&INZ)S1=7_{S%4CTY<Z5aqGt%Z$FP63un$DxFnzwVC8k#B|u)0pq>fg*J8aDxBQ)_Ssc1T}4w&e2A)NRVATwCC!5*uB__xyto4Ah>YU;{2~RmEb{H7#()rlxrJj_r^>--PKuRf5LkhY0*<-!2=!{0miI%KT~aB>+NoP$4GpP(d`0^;e7`s^!4CsCX1nx~y0WhpgJh33gcjllMRP7#Xh38E<|JLT|jc+qSFmq6!X0TAMEp5KF!LN|DkNMN01!W#n}GI}RB_it|8|PEPpKWFG)RJ0}|Y^nqkhStn*fb&A<X)ur%abhQs`K7~Z66j35nTINQ}2feVn$3L>lW5UeBDzk};(%C2=u)i*`Yk&PnpZmeJ53DILDmssPH!AwSymWo<|BkG__g?`O@(hYXYhT9xjZY~W^q8VSKRt%uHdD`LiU8H5bp|v#?#_T9imX2bj>kCjJznbRsWMssExgDKOq_TPaSeElG>z8{HS9s3*GRs(@v6L@I4#0^yo$CvpedPlMyZHy^BV1Q7hXJ4lP$0MZ~%w6guIrTw20ROu9=puwu35u+U2WpAWuCpdFm<0<AE=+n|uE&Ew8FPc!fA*1w&Q7%wIapvh;;|VyE9?&oPH;0hQy#gxhC`qwO<trt7O&P(tHMrpbzZVUF+}_SagDwv(%=Oxx~`6kA!|-`8@syx6quoOlg!4tU9t-1>IhG?mykND$iwNX9lO$a}?R$bEpNHZQL!pWW)%21%Va=C-<y7aCs=Jilm7=!Td=bfE7pPah@_+VK*jV`I8EdvOn3FAq`b*?g&vH2W=Zn#2YH+p4<YP#><uA+8}f+;D(Oj-ADl?<2qZ0?%`W+frWorv0PlYC6#|th6NO;ye0%2k#V@SOO}#S{Xi-(XP~|A~oJl$rz#KZqq+*DC*sqQ_95LyQ-9JcFo&#*fFZht~_Y5A<iMYrC`lxS2Wc6rR311E|M^IsSDnlkmN76TxCc3>*%<+V5{*!j9;+^*y{T@mE-e6XguGPXJ>-x6jtGHJ)IswvvevX^5|fBP*c^+k)P@G7uh#ki$}jDU%xZu@IWfBb}9D<g->Hz?Kk9jIbB;CRy|6PW94;<1LZZO?<zgTtV)Vm72q`-lcjE!_&3|7_RjGY3yEwyh~1@f(3!sKc+8BA-w-b`2k78<Lwq}gOhJ|3X&KCWD_bqqgEC*9Lrfl^zPj$M{L{P9*HV|*)l!%A&7>#5(C49~q0d9et9g$-3*pP~Riol~&v;frSym7X@VUh90H0G@-osbKDP~pfm3@6d@(PGR;Zp}1Vf<PaKwt)_p5Lg?n@JxxlfI;3b?pK<R8$#KsHoPr=mQ_8Z#NU5EkACnw2tP*{F4WpB345T0V`a4mie10YZ!~6oWl@1F|JCArZErn1*@8!gV$D_eMN^N#)#DrQ^1PeGeHh5%<%<SF*X*Gc>rt2w0yg-(CL=Y>*-C7!_Vy4_zVtlbO!T#s^54k%ny%+d3hnE)xBxwPQF!ANGKj4$}XV%u$hLKVnF#_iz~%|lomjms{vF|KkrTkeQ>#R<oaY%j}ksp!beJ7`Wots8gCx4C=QRDh>06Z`e0UOZ$MByKvkS`RvzB6E^!F}l|v_`S3u!Qjqn_0k!8Nv5iQMMFd+oz{wJjBF^LmtFL4c-;Ku!5V_R}Gak)J&P?*tiJO=V(g)t>4dj71kea14_{$srlvPquJN3`upRf~Dty5o|q*DIDdRY5StA<lbP#l$=B_uF%zm^Ae;%<)rp3$+2;A8|Ha86`Li#k$U)nJEr&q^X8f-i%Jc(8bHF^R~-s^m!hw@Bexfix;%(t752(*$;lttzR+wRL;Q>@1}SP!@xOliB0ijOmW>Vi|qP{MUVefuG{R?DFAG%wR-IOGpNMux>!SYwBr*ChX<1c!+<BL8q=KavE?t!P0mj31ac|XC+@v6G(LT&I5>T9xG}G>DT*K9324SciA~6WAqcOSe1+e?VHElA;ZmoLtF5Yo3r(BE3D*#7h-<lCgAhWftIn~+x7`FQu4YKXx6vrBo&h14#lu4>dTO}D?o&hQ0R-I0JIeqVYb}{0EIA{Y2#e^q&Py}Y9II2vyeN-v%n_hV`~WDwrzm8N?>e3LPe}$(K6)R0if7n$E1qgBsxf9=B5ibuDZn$NuXs|4`J}}J%^)eZqe!6cBr8CFR>iEu+@Jfl=OqIwlXL4Sn?@YRgz-2Me2Fz+m)B8K!L1huH(5h#G(%dq85Vg;?qCGT?bp#~NM_wO5}HDE;&*inEL(7uqXiH|xP}-5T!$z0`m#X4F|V`v?2|5sFN}d6z;T$UsfQ>>mm7(E;!9itCcKrjksspY^=S*$^__}FrOqGuP#TGxGau?tW|ogU#K$jjbj0MnsOHxK@&V!UDokYKyd{77&`Bvfw_E?8L66VUIBVt8#S-$Njr?HBaef))CBKZ=#4fQy*)u1biF5r;nYVcr6_1xG_$z0N@|f2UN4yRxzUrV9XJ)kDZS1IEHaeT3e7v`~cxR7Gby-DY1b)RFv*PzK0g1-SX}s0l09E6vG@VgDwY{m?v(*74lsrIThu2?m2>}6YC~u6TcX^t#&=<(QZtC5a`=AP|8&l2>I!CH4fa=khCA7VVII_L#areDzr-vPs*Bvy<IcM)JZCaEf%2=9R+0!Y;bHyI8Y^UV3)idA70ILrsQZ<vfXJA<0pQ4)U6nnc=bp;0YUGrOSuA`_PAXN`goQ{8qO9*Huv#$MWWBpyydf%~4D5&4Q)e#nN2Vu#@0D{9~4IUIOarFUpY1s&uz{3to>kdL2UXl;2Y-$_C21$spSUuFEAQ0=JQ|#&?J|?-!`_tT$5t$Y(1X7(Wop0~4<{dhFhQwqR^4WeYFp4g*1bFax0Dp@iVTv6yOp9ZNU=pDFc)ZOTs#_nD0=tdf`y`d@SdoQI2$z4HQH+3<IB0T7-z+1@6mBS>I%}am?rsGJ_Z{_x7AoSrh4<BB0kO%Mm;eoN4FDa|S63fK6{Qb|)2xL+%wsR)dou4AmJuhXaH_4A<N@lr%q0fY#T){H`mUzWsRaI!!x|<LVm39fm<oTI%S4RcY3t9VmX#b%S$NuHZALb+lGWk!S;eoFh;sxN$<&oK<(76lT=-9Ex=DIxv*rfkXve&Vt2%W+ZPmDK{Nq@JXHQK^<{g)=gNK3#6DYBXBqdIhtHzupm2X{K0nf#4t1c_dk-dG1F=Vy8EClA{gM(652Zb{|M8B!>*Q}hD8qJkU;Ib+YI$h)X1O92K_!<u2hfou*p(ZIN956~D|4ap)+_<)COv~|M@=Qe@sq6$O^&zOOYf!tI3QsEaF>i;PPD|sbO5XNs#SH{V7H98_c8L^GvdtFnl<Hn?1*V5I=j9nUlc_pyzhC;+1Q!tx(8xP8fG{k5y2Kg+T6&N8fXc%&Wyu;o1PEl4odfCL?ScRMe}Olb6a'
REFERENCE_SHA256 = '83cb2474f77f8f6ba7ccbffd5311b656e23f3b103942be25f564bb0271e98b23'
RESULT_NAME = 'EURCHF_H1_SHORT_PASS1_ENGULFING_DISCOVERY_RESULTS.zip'
OUT = Path(os.getenv('EURCHF_PASS1_OUTPUT_DIR', '/tmp/eurchf_h1_short_pass1')).resolve()
PREFIX = '/eurchf-h1-short-pass1'
LOCK = threading.RLock()
STARTED = False
JOB_LOCK = None
RUN_CLOCK = None
STATUS = dict(state='not_started', progress=0, message='Ready', version=VERSION,
              orders_supported=False, trading_enabled=False, pair=PAIR)


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
    rows = list(rows)
    if fields is None:
        fields = list(dict.fromkeys(key for row in rows for key in row))
    sink = CSVFile(path, fields)
    try:
        for row in rows:
            sink.add(row)
    finally:
        sink.close()


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


def config_key(config):
    return tuple(config[k] for k in ('lookback', 'distance_atr', 'body_min_atr', 'range_min_atr', 'close_max'))


def make_configs():
    configs, by_key, membership = [], {}, []

    def add(label, role, lookback=0, distance=None, body=0., candle_range=0., close_max=None):
        row = dict(config_id=label, lookback=lookback, distance_atr=distance,
                   body_min_atr=body, range_min_atr=candle_range, close_max=close_max)
        key = config_key(row)
        if key not in by_key:
            by_key[key] = row
            configs.append(row)
        row = by_key[key]
        membership.append(dict(requested_label=label, stage_group=role, config_id=row['config_id']))

    add('RAW_ENGULF', 'CONTROL_RAW')
    add('SCREEN_CONTROL', 'CONTROL_ARCHIVED', 60, .25, .75, 1.25)
    for x in (.25, .50, .75, 1., 1.25, 1.50):
        add(f'BODY_{round(x*100):03d}', 'SINGLE_BODY', body=x)
    for x in (.50, .75, 1., 1.25, 1.50, 1.75, 2.):
        add(f'RANGE_{round(x*100):03d}', 'SINGLE_RANGE', candle_range=x)
    for x in (.10, .20, .25, .33, .50):
        add(f'CLOSE_{round(x*100):03d}', 'SINGLE_CLOSE', close_max=x)
    for lb in LOOKBACKS:
        for distance in DISTANCES:
            add(f'STRUCT_L{lb:03d}_D{round(distance*100):03d}', 'SINGLE_STRUCTURE', lb, distance)
    for lb in LOOKBACKS:
        for distance in DISTANCES:
            for body in BODIES:
                for candle_range in RANGES:
                    label = f'M_L{lb:03d}_D{round(distance*100):03d}_B{round(body*100):03d}_R{round(candle_range*100):03d}'
                    add(label, 'STRUCTURE_BODY_RANGE_MATRIX', lb, distance, body, candle_range)
    assert len(configs) == 619 and len({c['config_id'] for c in configs}) == 619
    assert len(membership) == 644
    return configs, membership


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


def make_features(bars):
    atr = atr14(bars)
    prev = {lb: previous_high([b[2] for b in bars], lb) for lb in LOOKBACKS}
    raw = {}
    for i in range(WARMUP, len(bars)):
        if not bearish_engulf(bars, i) or atr[i] is None or atr[i] <= 0:
            continue
        op, hi, lo, cl = bars[i][1:]
        row = dict(signal_index=i, signal=iso(bars[i][0]), entry=iso(bars[i][0] + HOUR),
                   atr14=atr[i], body_atr=abs(cl-op)/atr[i], range_atr=(hi-lo)/atr[i],
                   close_location=(cl-lo)/(hi-lo), reference_stop_pips=(hi + 10*TICK - cl)/PIP)
        for lb in LOOKBACKS:
            row[f'previous_high_{lb}'] = prev[lb][i]
            row[f'signed_distance_atr_{lb}'] = (hi-prev[lb][i])/atr[i]
            row[f'abs_distance_atr_{lb}'] = abs(hi-prev[lb][i])/atr[i]
        raw[i] = row
    return raw


def selected_indices(config, features):
    result = []
    for i, row in features.items():
        if row['body_atr'] < config['body_min_atr'] or row['range_atr'] < config['range_min_atr']:
            continue
        if config['lookback'] and row[f"abs_distance_atr_{config['lookback']}"] > config['distance_atr']:
            continue
        if config['close_max'] is not None and row['close_location'] > config['close_max']:
            continue
        result.append(i)
    return result


def find_paths(bars, i):
    """Fixed stop/target first-touch is reusable; eligibility is replayed per cost."""
    reference = bars[i][4]
    stop = bars[i][2] + 10*TICK
    target = reference - RR*(stop-reference)
    common = dict(signal_index=i, signal=iso(bars[i][0]), entry=iso(bars[i][0]+HOUR),
                  reference_entry=reference, stop=stop, target=target,
                  next_open=bars[i+1][1] if i+1 < len(bars) else None,
                  next_candle_delay_hours=(bars[i+1][0]-bars[i][0]-HOUR).total_seconds()/3600 if i+1 < len(bars) else None)
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
        legacy = dict(common, exit_index=j, exit=iso(bars[j][0]+HOUR),
                      exit_price=stop if legacy_reason == 'STOP' else target,
                      reason=legacy_reason, dual_touch=int(both), gap_stop=0, gap_target=0)
        # Opening prices are observed MID prints, not guaranteed broker fills.
        if op >= stop:
            reason, price, exit_time = 'STOP_GAP' if op > stop else 'STOP', max(op, stop), bars[j][0]
        elif op <= target:
            reason, price, exit_time = 'TARGET_GAP_CAPPED', target, bars[j][0]
        elif hit_stop:
            reason, price, exit_time = 'STOP', stop, bars[j][0]+HOUR
        else:
            reason, price, exit_time = 'TARGET', target, bars[j][0]+HOUR
        stress = dict(common, exit_index=j, exit=iso(exit_time), exit_price=price, reason=reason,
                      dual_touch=int(both), gap_stop=int(op > stop), gap_target=int(op <= target))
        return {'SCREEN_PARITY': legacy, 'STOP_FIRST_GAP_STRESS': stress}
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


def fetch_history():
    token = os.getenv('OANDA_TOKEN', '').strip()
    if not token:
        raise RuntimeError('OANDA_TOKEN is not configured on this research service.')
    if any(ord(c) < 32 or ord(c) == 127 for c in token):
        raise RuntimeError('OANDA_TOKEN contains a control character; re-enter the token without quotes/newlines.')
    api = os.getenv('OANDA_API_URL', 'https://api-fxtrade.oanda.com').rstrip('/')
    if api not in ('https://api-fxtrade.oanda.com', 'https://api-fxpractice.oanda.com'):
        raise RuntimeError('OANDA_API_URL must be the official fxtrade or fxpractice HTTPS API host.')

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            raise RuntimeError('Unexpected OANDA redirect; credentials were not forwarded.')

    opener = urllib.request.build_opener(NoRedirect)
    cursor, by_time, chunk = START, {}, 0
    total = math.ceil((END-START).days/180)
    while cursor < END:
        end = min(cursor+timedelta(days=180), END)
        params = dict(price='M', granularity='H1', smooth='false', includeFirst='true',
                      **{'from': iso(cursor), 'to': iso(end)})
        url = api + '/v3/instruments/EUR_CHF/candles?' + urllib.parse.urlencode(params)
        payload = None
        for attempt in range(3):
            request = urllib.request.Request(url, headers={'Authorization': 'Bearer '+token, 'Accept': 'application/json'}, method='GET')
            try:
                with opener.open(request, timeout=45) as response:
                    payload = json.load(response)
                break
            except urllib.error.HTTPError as exc:
                if exc.code not in (429, 500, 502, 503, 504) or attempt == 2:
                    raise RuntimeError(f'OANDA candle request failed: HTTP {exc.code}, chunk {chunk+1}.') from None
            except (urllib.error.URLError, TimeoutError):
                if attempt == 2:
                    raise RuntimeError(f'OANDA candle request timed out/failed after three attempts, chunk {chunk+1}.') from None
            time.sleep(attempt+1)
        if not isinstance(payload, dict) or payload.get('instrument') != PAIR or payload.get('granularity') != TIMEFRAME:
            raise RuntimeError('Unexpected OANDA candle response instrument/granularity.')
        candles = payload.get('candles')
        if not isinstance(candles, list) or not candles:
            raise RuntimeError(f'Empty/missing candles in historical chunk {chunk+1}.')
        for candle in candles:
            t = when(candle['time'])
            if not START <= t < END or not candle.get('complete'):
                continue
            mid = candle.get('mid')
            if not mid:
                raise RuntimeError('Completed H1 candle has no MID OHLC.')
            row = (t, *[float(mid[k]) for k in ('o','h','l','c')])
            if t in by_time and by_time[t] != row:
                raise RuntimeError('Conflicting duplicate OANDA candle at '+iso(t))
            by_time[t] = row
        chunk += 1
        set_status(state='fetching', progress=round(2+18*chunk/total), message=f'EUR/CHF H1 historical chunk {chunk}/{total}')
        cursor = end
    return [by_time[t] for t in sorted(by_time)]


def validate_bars(bars):
    if not bars:
        raise RuntimeError('Empty dataset.')
    for i,(t,op,hi,lo,cl) in enumerate(bars):
        if t.tzinfo is None or t.minute or t.second or t.microsecond:
            raise RuntimeError('Candle time is not a UTC whole hour.')
        if not all(math.isfinite(x) and x > 0 for x in (op,hi,lo,cl)) or not lo <= min(op,cl) <= max(op,cl) <= hi:
            raise RuntimeError('Invalid OHLC at '+iso(t))
        if i and t <= bars[i-1][0]:
            raise RuntimeError('Unsorted or duplicate candle timestamp.')
        if not START <= t < END:
            raise RuntimeError('Candle outside the frozen study window.')


def reference_rows():
    raw = zlib.decompress(base64.b85decode(REFERENCE_B85))
    if sha(raw) != REFERENCE_SHA256:
        raise RuntimeError('Embedded screen reference is corrupt.')
    return json.loads(raw)


def hard_controls(bars, features, paths, configs):
    rows = []

    def check(name, actual, expected):
        rows.append(dict(check=name, status='PASS' if actual == expected else 'FAIL', actual=actual, expected=expected))

    check('source_candle_count', len(bars), EXPECTED_CANDLES)
    check('source_first', iso(bars[0][0]), '2005-01-02T18:00:00Z')
    check('source_last', iso(bars[-1][0]), '2026-09-30T23:00:00Z')
    check('source_full_OHLC_SHA256', source_hash(bars), EXPECTED_SOURCE_SHA256)
    control = next(c for c in configs if c['config_id'] == 'SCREEN_CONTROL')
    indices = selected_indices(control, features)
    check('screen_raw_count', len(indices), EXPECTED_RAW)
    check('screen_raw_signal_SHA256', signal_hash(indices, bars), EXPECTED_SIGNAL_SHA256)
    expected = reference_rows()
    for cost in COSTS:
        accepted, invalid, _ = replay(indices, paths, 'SCREEN_PARITY', cost)
        completed = [i for i in accepted if paths[i]['SCREEN_PARITY']['exit'] is not None]
        check(f'screen_completed_{cost}T', len(completed), EXPECTED_ACCEPTED)
        check(f'screen_invalid_geometry_{cost}T', len(invalid), 0)
        check(f'screen_open_{cost}T', len(accepted)-len(completed), 0)
        refs = [r for r in expected if int(r['cost_ticks']) == cost]
        mismatch, largest_error = 0, 0.
        if len(refs) != len(completed):
            mismatch += abs(len(refs)-len(completed))
        for i,ref in zip(completed,refs):
            path = paths[i]['SCREEN_PARITY']
            actual = dict(path, cost_ticks=cost, historical_fill=geometry(path,cost)[0], r=r_value(path,cost))
            for key in ('signal_index','exit_index','cost_ticks'):
                mismatch += int(int(actual[key]) != int(ref[key]))
            for key in ('signal','entry','exit','reason'):
                mismatch += int(actual[key] != ref[key])
            for key in ('reference_entry','historical_fill','stop','target','r'):
                error = abs(float(actual[key])-float(ref[key]))
                largest_error = max(largest_error,error)
                mismatch += int(error > 1e-10)
        check(f'screen_full_ledger_field_mismatches_{cost}T', mismatch, 0)
        rows.append(dict(check=f'screen_max_numeric_error_{cost}T', status='PASS' if largest_error <= 1e-10 else 'FAIL', actual=largest_error, expected='<=1e-10'))
    return rows


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
    while True:
        point = datetime(year,month,1,tzinfo=UTC)
        if point > END:
            break
        result.append(point)
        year, month = (year+1,1) if month == 12 else (year,month+1)
    return result


BOUNDS = month_bounds()


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


PERIODS = period_definitions()


PROTOCOL = """# EUR/CHF H1 SHORT Pass 1 — predeclared design

## Purpose and guide
Use the 24 September AUD/JPY research template as a guide to disciplined
discovery, with the user's 1 October clarification that justified adaptations
are welcome. Test whether exact bearish engulfing near prior highs contains a
coherent region after costs. Do not freeze the largest backtest row as a strategy.

619 unique geometries: raw engulf and archived screen controls; single body,
range, strong-close and structure studies; a 600-cell structure/body/range matrix.
644 study memberships include 25 duplicate geometries, evaluated only once.
Full grid levels and memberships are exported, including weak and empty cases.
Zero body/range thresholds mean no such filter. Structure uses ABSOLUTE distance
of signal high to the maximum of the PREVIOUS L H1 candles (current excluded).
Signed distances are exported for interpretation, without a new sign filter.

## Frozen execution and data
OANDA completed unsmoothed MID H1; EUR_CHF short only; 2005-01-01 through
2026-10-01 exclusive. The first 200 observed candles warm all entry studies.
ATR14 is Wilder recursion, seeded with TR[1:15]; the signal's completed ATR is
used. Exact bearish engulf: previous close>open, current close<open,
current open>=previous close, current close<=previous open. No session, weekday,
daily-trend, policy-date exclusion or RR search. One standalone configuration
at a time; no currency conversion, NAV compounding or portfolio admission here.

Signal timestamp = candle start; reference entry = its close, timestamped one
hour later. Stop = signal high + 0.00010 (10 ticks = 1 pip). Target = reference
close - 3*(stop-reference close). Assumed short fill = reference close minus
10/20/40 ticks (1/2/4 pips); target/stop stay at reference geometry. Risk
denominator = stop minus assumed fill. An invalid nonpositive target or target
at/above assumed fill is rejected BEFORE p0, and is exported. Entry at/after
the frozen cutoff is also ineligible. This matters for small raw candles.
Cost assumptions are scenarios, not measured quotes or a maximum realized loss.

p0 is replayed for EVERY geometry/model/cost; open-at-cutoff trades occupy the
strategy and block later signals. Exit-candle signals may enter at that candle's
close. Fixed price barriers permit cached first-touch paths; rejected fill
geometry and occupancy are still recomputed independently at each assumed cost.
Completed ledgers alone are never obtained by filtering an earlier p0 ledger.

SCREEN_PARITY retains the old screen: next-candle onward OHLC barrier test;
if both barriers touch, nearest-to-open extreme is assumed first, tie stop-first;
barrier fills and exit-candle-end timestamps. It must match the full archived
240-trade screen at all three costs, as well as source/raw-signal fingerprints.
Any changed source/history/control stops the run before discovery results.

STOP_FIRST_GAP_STRESS uses the same signals/entry assumptions: opening price
at/above stop exits at max(open,stop), opening at/below target fills at target
(no favorable improvement), otherwise dual-touch bars lose. Known opening-gap
exits are timestamped at that candle's start; other exits at its end. This is a
sensitivity model, NOT a worst-case bound: MID H1 cannot reveal intrabar jump
execution, spread blowouts, actual quotes, financing, or fills during closure.
Both models disclose next observed open and any delay after assumed entry.

## Pair-specific adaptation
Add pre-floor, floor-period, 15 January 2015 and post-event diagnostics and
identify every accepted trade exposed during that day. All dates are retained
in full-history results. These are event/cohort summaries, NOT counterfactual
trade deletions or date filters. The SNB introduced the minimum EUR/CHF rate on
6 September 2011 and discontinued it on 15 January 2015. Day-level boundaries
do not claim the precise intraday announcement time.
Primary sources:
https://www.snb.ch/en/publications/communication/press-releases/2011/pre_20110906
https://www.snb.ch/en/publications/communication/press-releases/2015/pre_20150115
API candle-parameter reference: https://developer.oanda.com/rest-live-v20/pricing-ep/
The candle-only request route is retained from the successfully run Pair #9 code.

## Reporting definitions
Full accepted ledgers are normalized for a small results ZIP. Join
accepted_trades.csv to signal_trade_paths.csv on (signal_index, execution_model).
Each accepted row supplies config_id, cost, actual assumed fill, risk and R;
the path supplies signal/entry/exit times, stop, target, reason and exit price.
Open accepted trades remain in the table with blank exit/R, not a zero result.
control_accepted_ledgers.csv additionally contains denormalized raw and archived
control ledgers. Source candles, features and raw signal membership are exported.

Candidate-only figures treat qualifying signals in isolation and can overlap;
they are NOT executable strategy/portfolio results. Accepted metrics use full
p0 chronology. Trade comparisons to RAW and SCREEN use actual accepted signal
sets at the same cost/model; adds and removals are descriptive full-replay
differences, not marginal trades assumed independent of displacement.

Entry cohorts use [period start, period end); their eventual R is not realized
period return. Exit cash uses (period start, period end], including a final
completed candle's close at the cutoff. Both are reported. Monthly data and
all rolling 12/24/36-month realized-R windows include zero-trade months;
incomplete 2026 is labeled. Rolling R is additive R, not compounded percent.
No realized R is invented for open trades, and no mark-to-market is inferred.
Last 1/2/3/5/10 years end at the fixed cutoff. Eras are 2005-09, 2010-15,
2016-21 and 2022-cutoff. They are descriptive, previously inspected history,
not pristine out-of-sample tests. Source gaps are reported, never interpolated.

## Next decision
Review stressed-cost coherence, neighbours, frequency, era and recent weakness,
execution sensitivity and policy-event concentration. Freeze only a few
distinguishable anchors if justified; then test conditional features. A weak
screen does not exhaust a pair; broad unsupported discovery is not a reason
for unlimited searching. Entry geometry should become intelligible and freeze
before later RR work, independent ledger confirmation and exact P31 admission.
No order endpoints, live executor mutations, automatic strategy selection,
portfolio rankings or claims of prospective profitability are in this runner.
"""


def write_inputs(work, bars, features, paths, configs, memberships, dataset_kind):
    write_csv(work/'source_candles.csv',
              [dict(time=iso(t),open=op,high=hi,low=lo,close=cl) for t,op,hi,lo,cl in bars])
    write_csv(work/'coverage.csv', [dict(pair=PAIR,timeframe=TIMEFRAME,price_type='MID',
              requested_start=iso(START),end_exclusive=iso(END),candles=len(bars),
              first=iso(bars[0][0]),last=iso(bars[-1][0]),warmup_bars=WARMUP,
              sha256=source_hash(bars),dataset_kind=dataset_kind)])
    write_csv(work/'data_gaps.csv',
              [dict(previous_candle=iso(a[0]),next_candle=iso(b[0]),
                    missing_wall_clock_hours=(b[0]-a[0]).total_seconds()/3600-1,
                    interpretation='Includes ordinary market closures; not automatically missing broker data')
               for a,b in zip(bars,bars[1:]) if b[0]-a[0] > HOUR],
              ['previous_candle','next_candle','missing_wall_clock_hours','interpretation'])
    feature_fields = ['signal_index','signal','entry','atr14','body_atr','range_atr','close_location','reference_stop_pips']
    feature_fields += [k for lb in LOOKBACKS for k in (f'previous_high_{lb}',f'signed_distance_atr_{lb}',f'abs_distance_atr_{lb}')]
    write_csv(work/'raw_signal_features.csv', features.values(), feature_fields)
    write_csv(work/'configuration_grid.csv', configs)
    write_csv(work/'configuration_memberships.csv', memberships)
    path_fields = ['execution_model','signal_index','signal','entry','reference_entry','stop','target',
                   'next_open','next_candle_delay_hours','exit_index','exit','exit_price','reason','dual_touch','gap_stop','gap_target']
    write_csv(work/'signal_trade_paths.csv',
              [dict(execution_model=model,**paths[i][model]) for i in paths for model in MODELS], path_fields)
    write_csv(work/'period_definitions.csv', [dict(period=label,period_type=kind,start=iso(a),end=iso(b),
              entry_interval='[start,end)',exit_cash_interval='(start,end]',partial_calendar_year=int(kind=='calendar' and b==END and END.month!=1))
              for label,kind,a,b in PERIODS])


STAT_FIELDS = list(stats([]))
CASE = ['config_id','execution_model','cost_ticks']
SUMMARY_FIELDS = CASE + ['raw_signals','eligible_isolated_signals','isolated_completed',
    'isolated_open','isolated_total_r','isolated_profit_factor','geometry_invalid_all_signals',
    'p0_blocked_signals','invalid_unblocked_entries','open_at_data_end'] + STAT_FIELDS + [
    'dual_touch_closed','gap_stop_closed','gap_target_closed','entry_next_open_gap_count',
    'entries_before_market_closure','median_stop_pips','median_cost_fraction_reference_risk',
    'p90_cost_fraction_reference_risk','median_holding_hours','max_holding_hours',
    'maximum_inter_entry_gap_days','leading_no_entry_days','trailing_no_entry_days',
    'zero_entry_months','max_consecutive_zero_entry_months','positive_complete_entry_years',
    'negative_complete_entry_years','zero_entry_complete_years','raw_signal_sha256','accepted_ledger_sha256']
for _width in (12,24,36):
    SUMMARY_FIELDS += [f'worst_{_width}m_realized_r',f'worst_{_width}m_start',f'worst_{_width}m_end',f'zero_entry_{_width}m_windows']


def analyze(work, bars, features, paths, configs, *, progress=True):
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
        'period_results.csv': CASE+['period','period_type','start','end','partial_calendar_year','entry_count',
             'entry_cohort_completed','entry_cohort_eventual_r','entry_cohort_open',
             'exit_count','realized_r','realized_max_dd_r','open_at_period_end'],
        'monthly_results.csv': CASE+['month','entry_count','exit_count','realized_r','entry_cohort_eventual_r','entry_cohort_open'],
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
    path_times = {(i,model):(when(p['entry']),when(p['exit']) if p['exit'] else None)
                  for i,models in paths.items() for model,p in models.items()}
    # Trade R only depends on signal path and assumed fill; p0 is still replayed each time.
    outcome = {(model,cost):{i:r_value(paths[i][model],cost) for i in paths} for model in MODELS for cost in COSTS}
    refs = {}
    for config in configs:
        if config['config_id'] in ('RAW_ENGULF','SCREEN_CONTROL'):
            indices = selected_indices(config,features)
            for model in MODELS:
                for cost in COSTS:
                    refs[(config['config_id'],model,cost)] = replay(indices,paths,model,cost)[0]
    shock_start = datetime(2015,1,15,tzinfo=UTC)
    shock_end = datetime(2015,1,16,tzinfo=UTC)
    try:
        for number,config in enumerate(configs,1):
            cid = config['config_id']
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
                        if cid in ('RAW_ENGULF','SCREEN_CONTROL'):
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
                            entry_count=month_entries[k],exit_count=month_exits[k],realized_r=month_cash[k],
                            entry_cohort_eventual_r=month_cohort[k],entry_cohort_open=month_open[k]))
                    positive_years = negative_years = empty_years = 0
                    for label,kind,a,b in PERIODS:
                        entry_ids = [i for i in accepted if a <= path_times[(i,model)][0] < b]
                        exit_ids = [i for i in closed if a < path_times[(i,model)][1] <= b]
                        cohort_closed = [i for i in entry_ids if rs_by_i[i] is not None]
                        cohort_r = sum(rs_by_i[i] for i in cohort_closed)
                        period_stats = stats(rs_by_i[i] for i in exit_ids)
                        opened = sum(path_times[(i,model)][0] <= b and
                                     (path_times[(i,model)][1] is None or path_times[(i,model)][1] > b) for i in accepted)
                        partial = int(kind == 'calendar' and b == END and END.month != 1)
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
                    for reference in ('RAW_ENGULF','SCREEN_CONTROL'):
                        reference_ids = refs.get((reference,model,cost))
                        if reference_ids is None:
                            continue  # only used by tiny synthetic subset tests
                        ours, theirs = set(accepted),set(reference_ids)
                        common, added, removed = ours & theirs, ours-theirs, theirs-ours
                        total = lambda ids: sum(rs_by_i[i] for i in sorted(ids) if rs_by_i[i] is not None)
                        reference_r = total(theirs)
                        sinks['accepted_comparisons.csv'].add(dict(tag,reference_config=reference,
                            common_entries=len(common),added_entries=len(added),removed_entries=len(removed),
                            common_completed_r_candidate=total(common),common_completed_r_reference=total(common),
                            added_completed_r=total(added),removed_completed_r=total(removed),
                            candidate_total_r=sum(rs),reference_total_r=reference_r,delta_total_r=sum(rs)-reference_r,
                            candidate_open_count=len(open_ids),reference_open_count=sum(rs_by_i[i] is None for i in reference_ids)))
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
    counts = {name:sink.count for name,sink in sinks.items()}
    write_json(work/'output_row_counts.json',counts)
    return summaries,counts


def write_neighbours(work, configs, summaries):
    axes = {'lookback':LOOKBACKS,'distance_atr':DISTANCES,'body_min_atr':BODIES,'range_min_atr':RANGES}
    grid = {config_key(c):c for c in configs if c['lookback'] in LOOKBACKS and c['close_max'] is None
            and c['body_min_atr'] in BODIES and c['range_min_atr'] in RANGES and c['distance_atr'] in DISTANCES}
    rows = []
    for config in grid.values():
        neighbours,edges = set(),[]
        for name,levels in axes.items():
            index = levels.index(config[name])
            if index in (0,len(levels)-1):
                edges.append(name+('=LOWER' if index==0 else '=UPPER'))
            for adjacent in (index-1,index+1):
                if 0 <= adjacent < len(levels):
                    other = dict(config,**{name:levels[adjacent]})
                    if config_key(other) in grid:
                        neighbours.add(grid[config_key(other)]['config_id'])
        for model in MODELS:
            for cost in COSTS:
                values = [summaries[(cid,model,cost)] for cid in sorted(neighbours)]
                rows.append(dict(config_id=config['config_id'],execution_model=model,cost_ticks=cost,
                    tested_boundaries=';'.join(edges),adjacent_configurations=len(values),
                    positive_total_r_neighbours=sum(r['total_r'] > 0 for r in values),
                    median_neighbour_total_r=quantile([r['total_r'] for r in values],.5),
                    minimum_neighbour_total_r=min((r['total_r'] for r in values),default=None),
                    minimum_neighbour_closed_trades=min((r['closed_trades'] for r in values),default=None),
                    neighbour_ids=';'.join(sorted(neighbours))))
    write_csv(work/'neighbourhood_summary.csv',rows,
              CASE+['tested_boundaries','adjacent_configurations','positive_total_r_neighbours',
                    'median_neighbour_total_r','minimum_neighbour_total_r','minimum_neighbour_closed_trades','neighbour_ids'])


def self_checks():
    """Small mathematical/execution checks; no claims about market performance."""
    passed = []
    def check(condition, name):
        if not condition:
            raise AssertionError(name)
        passed.append(dict(check=name,status='PASS',evidence='SYNTHETIC_SOFTWARE_ONLY'))
    t = datetime(2020,1,1,tzinfo=UTC)
    bars = [(t,10.,11.,9.,10.5),(t+HOUR,10.5,11.,9.,9.5)]
    check(bearish_engulf(bars,1),'Exact bearish engulf accepts boundary equality')
    check(not bearish_engulf([(t,10.,11.,9.,10.),bars[1]],1),'Previous doji rejected')
    vals = [1.,3.,2.,100.,4.,5.]
    ph = previous_high(vals,3)
    check(ph == [None,None,None,3.,100.,100.],'Prior extreme excludes signal high')
    for lb in (1,2,3,5):
        fast = previous_high(vals,lb)
        check(all(fast[i] == max(vals[i-lb:i]) for i in range(lb,len(vals))),f'Prior high agrees with direct slices LB{lb}')
    uniform = [(t+i*HOUR,10.,11.,9.,10.) for i in range(25)]
    atr = atr14(uniform)
    check(atr[:14] == [None]*14 and all(x == 2. for x in atr[14:]),'Wilder ATR seed and recursion')
    c,m = make_configs()
    check(len(c)==619 and len(m)==644,'Complete grid and duplicate membership control')
    check(len([x for x in c if x['config_id']=='SCREEN_CONTROL'])==1,'Screen control has one immutable geometry')
    # Signal close 10, stop 11.0001, target 6.9997. Both barriers; low nearer open.
    two = [(t,10.5,11.,9.,10.),(t+HOUR,7.2,11.2,6.8,8.)]
    paths = find_paths(two,0)
    check(paths['SCREEN_PARITY']['reason']=='TARGET' and paths['STOP_FIRST_GAP_STRESS']['reason']=='STOP',
          'Dual-touch stop-first diagnostic differs from archived heuristic')
    gapped = [(t,10.5,11.,9.,10.),(t+HOUR,12.,12.1,11.5,11.8)]
    gp = find_paths(gapped,0)
    check(gp['STOP_FIRST_GAP_STRESS']['exit_price']==12. and r_value(gp['STOP_FIRST_GAP_STRESS'],10)<-1,
          'Adverse opening gap can lose more than 1R')
    check(gp['STOP_FIRST_GAP_STRESS']['exit']==iso(t+HOUR),'Known opening-gap exit uses opening timestamp')
    capped = [(t,10.5,11.,9.,10.),(t+HOUR,6.,6.5,5.5,6.2)]
    cp = find_paths(capped,0)['STOP_FIRST_GAP_STRESS']
    check(cp['exit_price']==cp['target'] and cp['gap_target']==1,'Favorable target gap gives no improvement')
    # Explicit replay fixtures isolate p0 from barrier detection.
    fixture = dict(signal_index=0,signal=iso(t),entry=iso(t+HOUR),reference_entry=1.,stop=1.001,
                   target=.997,exit_index=None,exit=None,exit_price=None,reason='OPEN_AT_DATA_END',dual_touch=0,gap_stop=0,gap_target=0)
    mapping = {i:{model:dict(fixture,signal_index=i) for model in MODELS} for i in (0,1,2)}
    chosen,_,blocked = replay([0,1,2],mapping,'SCREEN_PARITY',10)
    check(chosen==[0] and blocked==2,'Open trade occupies p0 through data end')
    for model in MODELS:
        mapping[0][model].update(exit_index=1,exit=iso(t+2*HOUR),exit_price=1.001,reason='STOP')
    check(replay([0,1,2],mapping,'SCREEN_PARITY',10)[0]==[0,1],'Exit-candle signal can re-enter')
    mapping[0]['SCREEN_PARITY'].update(target=.9998)
    check(replay([0,1,2],mapping,'SCREEN_PARITY',40)[0]==[1],'Invalid high-cost entry does not occupy p0')
    check(replay([0,1,2],mapping,'SCREEN_PARITY',10)[0]==[0,1],'Cost-specific replay preserves valid lower-cost entry')
    check(geometry(dict(fixture,entry=iso(END)),10)[1]=='ENTRY_AT_OR_AFTER_CUTOFF','No new trade at exclusive data cutoff')
    check(r_value(fixture,10) is None,'Unclosed trade has no invented realized R')
    check(stats([])['closed_trades']==0 and stats([])['total_r']==0,'Empty case remains explicit')
    check(stats([2.,-1.,-1.,-1.,2.])['max_closed_dd_r']==-3.,'Closed R drawdown chronology')
    check(stats([-1.,-1.,2.,-1.])['max_losing_streak']==2,'Losing streak reset')
    check(len(BOUNDS)==262 and len(PERIODS)==35,'Complete zero-inclusive monthly and period universe')
    return passed


def package(work, completed):
    """Atomic publication. An error archive cannot include partial performance."""
    allowed_on_error = {'protocol.md','run_manifest.json','error_report.csv','hard_controls.csv',
                        'software_checks.csv','coverage.csv','data_gaps.csv','runner_source.py'}
    members = [p for p in sorted(work.iterdir()) if p.is_file() and p.name != 'file_manifest.json' and
               (completed or p.name in allowed_on_error)]
    files = [dict(file=p.name,bytes=p.stat().st_size,sha256=file_sha(p)) for p in members]
    write_json(work/'file_manifest.json',files)
    temporary = OUT / (RESULT_NAME+'.tmp')
    with zipfile.ZipFile(temporary,'w',zipfile.ZIP_DEFLATED,compresslevel=6,allowZip64=True) as z:
        for p in members+[work/'file_manifest.json']:
            z.write(p,p.name)
    os.replace(temporary,OUT/RESULT_NAME)


def run_job():
    global RUN_CLOCK, JOB_LOCK
    RUN_CLOCK = time.monotonic()
    OUT.mkdir(parents=True,exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='working-',dir=OUT))
    manifest = dict(version=VERSION,runner_sha256=code_hash(),pair=PAIR,side=SIDE,timeframe=TIMEFRAME,
        dataset_kind='HISTORICAL_OANDA_MID',status='RUNNING',complete=False,
        study='PASS1_ENTRY_DISCOVERY_ONLY',start=iso(START),end_exclusive=iso(END),
        rr=RR,cost_ticks=list(COSTS),execution_models=list(MODELS),
        expected_configurations=619,expected_cases=3714,reference_zip_sha256='786792e232d0477ed4c2d9440de7ec1110fd7568854b38edf2cb439c04c75fb0',
        incumbent_target='P31 frozen; not replayed/optimized in this standalone pass',
        template='FOREX_STRATEGY_RESEARCH_TEMPLATE_AUDJPY_2026-09-24.md',
        user_clarification='Template is a guide; justified pair-specific changes are allowed.',
        orders_supported=False,trading_enabled=False)
    try:
        (work/'protocol.md').write_text(PROTOCOL,encoding='utf-8')
        shutil.copyfile(__file__,work/'runner_source.py')
        checks = self_checks()
        write_csv(work/'software_checks.csv',checks)
        set_status(state='validating',progress=1,message='Local software controls passed; fetching frozen EUR/CHF history')
        bars = fetch_history()
        validate_bars(bars)
        # Export coverage even if the strict source gate fails.
        write_csv(work/'coverage.csv',[dict(pair=PAIR,candles=len(bars),first=iso(bars[0][0]),last=iso(bars[-1][0]),sha256=source_hash(bars))])
        if source_hash(bars) != EXPECTED_SOURCE_SHA256:
            write_csv(work/'hard_controls.csv',[dict(check='source_full_OHLC_SHA256',status='FAIL',
                expected=EXPECTED_SOURCE_SHA256,actual=source_hash(bars))])
            raise RuntimeError('EUR/CHF candle history differs from the supplied screen. Discovery stopped; inspect coverage and source hash before changing any reference.')
        configs,memberships = make_configs()
        features = make_features(bars)
        control_indices = selected_indices(next(c for c in configs if c['config_id']=='SCREEN_CONTROL'),features)
        paths = {i:find_paths(bars,i) for i in control_indices}
        hard = hard_controls(bars,features,paths,configs)
        write_csv(work/'hard_controls.csv',hard)
        if any(r['status']!='PASS' for r in hard):
            raise RuntimeError('Archived screen/full-ledger controls failed; no discovery conclusions are valid.')
        set_status(state='building_paths',progress=24,message='Archived screen full-ledger parity PASS; preparing raw engulf paths')
        for number,i in enumerate(features,1):
            if i not in paths:
                paths[i] = find_paths(bars,i)
            if number % 2000 == 0:
                set_status(state='building_paths',progress=round(24+14*number/max(1,len(features))),
                           message=f'{number}/{len(features)} raw engulf signal paths built')
        paths = dict(sorted(paths.items()))
        write_inputs(work,bars,features,paths,configs,memberships,manifest['dataset_kind'])
        (work/'README.md').write_text(RESULT_README,encoding='utf-8')
        manifest.update(source_sha256=source_hash(bars),raw_engulf_signals=len(features),
                        hard_controls='PASS',software_controls='PASS',protocol_sha256=sha(PROTOCOL.encode()))
        _,counts = analyze(work,bars,features,paths,configs)
        manifest.update(status='COMPLETE',complete=True,output_row_counts=counts,
                        completed_at=iso(datetime.now(UTC)),elapsed_seconds=round(time.monotonic()-RUN_CLOCK,1))
        write_json(work/'run_manifest.json',manifest)
        set_status(state='packaging',progress=96,message='All 3,714 cases complete; compressing source, ledgers and diagnostics')
        package(work,True)
        set_status(state='complete',progress=100,message='EUR/CHF H1 short Pass 1 complete; download the results ZIP',
                   hard_controls='PASS',configurations=619,cases=3714,result_path='/results',result_bytes=(OUT/RESULT_NAME).stat().st_size)
        return True
    except Exception as exc:
        manifest.update(status='ERROR',complete=False,error_type=type(exc).__name__,error=str(exc))
        write_json(work/'run_manifest.json',manifest)
        write_csv(work/'error_report.csv',[dict(error_type=type(exc).__name__,message=str(exc),traceback=traceback.format_exc())])
        try:
            package(work,False)
            result_path = '/results'
        except Exception:
            result_path = None
        set_status(state='error',progress=100,message=str(exc),result_path=result_path,hard_controls='NOT_COMPLETE')
        return False
    finally:
        shutil.rmtree(work,ignore_errors=True)
        if JOB_LOCK is not None:
            JOB_LOCK.close()
            JOB_LOCK = None


RESULT_README = """# EUR/CHF H1 SHORT Pass 1 results

Start with run_manifest.json (complete=true), hard_controls.csv (all PASS),
coverage.csv, protocol.md, then summary.csv. An error ZIP contains no partial
performance files and is not a valid research result. file_manifest.json hashes
every other packaged artifact, excluding itself.

configuration_grid.csv has 619 unique entry configurations. Memberships explain
which 644 requested control/single-factor/matrix rows share a geometry. There
are 3,714 complete configuration/cost/execution cases. No automatic winner is
declared. neighbourhood_summary.csv shows adjacent tested cells and boundaries;
neighbours share history/trades and are not independent statistical evidence.

For ANY full accepted ledger: select its configuration/model/cost rows from
accepted_trades.csv, and join signal_trade_paths.csv by signal_index and
execution_model. The join is many-to-one; preserve accepted_sequence. Price
risk = stop - historical_fill. The short R = (fill - exit_price)/risk for a
completed trade. Non-gap STOP is exactly -1. Open R is blank, not zero.
control_accepted_ledgers.csv has already joined ledgers for RAW_ENGULF and
SCREEN_CONTROL. raw_signal_membership.csv plus raw_signal_features.csv show
qualifying signals before occupancy and cost-specific validity. The ledger
digest in summary.csv hashes canonical JSON lines of the denormalized fields
in control_accepted_ledgers.csv (sort_keys=True, separators=(',',':')).

Period and monthly files separate entry-cohort eventual R from realized exit R.
rolling_windows.csv is every month-boundary 12/24/36-month REALIZED-R window,
including inactivity. It is not percent compounded return. Open trades and
unobserved quotes are not silently assigned an outcome. Cost/exit-model
comparisons use full replay, not removal of trades from a prior accepted stream.

The CHF event file is exposure attribution only. Eventual R on an exposed trade
need not have been earned on the event day. No policy dates are excluded.
MID prices/assumed fills are not executable quote history. Funding, actual
spreads, intra-hour gap fills and broker mechanics need later investigation.

Use the guide to identify coherent anchors for conditional research. These are
exploratory standalone results; P31 admission, RR selection, independent full
signal reimplementation and prospective execution checks remain later work.
"""


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
        set_status(state='starting',progress=0,message='Starting EUR/CHF H1 short discovery',runner_sha256=code_hash(),result_path=None)
    if background:
        threading.Thread(target=run_job,name='eurchf-pass1-research',daemon=True).start()
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
    if path=='/start' or (not STARTED and os.getenv('EURCHF_PASS1_AUTOSTART','1')=='1'):
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
        current = dict(service='EUR/CHF H1 SHORT Pass 1 discovery',version=VERSION,status='/status',results='/results',
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
        if os.getenv('EURCHF_PASS1_AUTOSTART','1')=='1':
            launch()
        print(f'{VERSION}: listening on {port}; /status and /results',flush=True)
        server.serve_forever()
    return 0


if __name__=='__main__':
    raise SystemExit(main())
