#!/usr/bin/env python3
"""AUD/JPY M15 SHORT — PASS 7: independent full-ledger confirmation.

RESEARCH ONLY. Does not import or modify live executor/strategy probe, does not
send a webhook, and contains no OANDA order request. Separate OANDA MID candle
fetch; independently computed ATR, previous-high, prior-16 rise, SELL exits.

Predeclared, NOT ranked/auto-selected:
 CORE_LB60_M1.50: prior 60 high, body>=1.50 ATR, range>=2.25 ATR,
                    16-bar preceding rise>=1.50 ATR.
 FREQUENCY_LB40_M1.75: prior 40 high, body>=1.50 ATR, range>=2.25 ATR,
                        16-bar preceding rise>=1.75 ATR.
 Both: bearish candle; high strictly above previous-high; close strictly
 below previous-high; RR 4.00 for provisional independent check, RR3.50
 frozen research control. Both 2 and 4 ASSUMED adverse pips; short stop =
 signal high + 10 ticks, target referenced to signal close, p0 per strategy;
 exit starts next M15, exit-candle reentry eligible, intrabar tie closer to
 bar open, STOP on equal. JPY tick=.001, pip=.01.

Fail closed on source count/coverage/fingerprint, raw signal digests or ANY
accepted-ledger field. Same repeatedly researched history, NOT independent OOS.
Portfolio28->29 historical admission is a separate gate because AUDJPY LONG
may overlap/conflict with this proposed AUDJPY SHORT in a nonhedging account.
"""
from __future__ import annotations
import base64
import bisect
import csv
import datetime as dt
import hashlib
import json
import math
import os
import threading
import time
import traceback
import zipfile
import zlib
from collections import deque
from pathlib import Path

import numpy as np
import requests
from flask import Flask, jsonify, send_file

app = Flask(__name__)
API = os.getenv('OANDA_API_URL', 'https://api-fxtrade.oanda.com').rstrip('/')
TOKEN = os.getenv('OANDA_TOKEN', '')
PAIR = 'AUD_JPY'
PIP = 0.01
TICK = 0.001
STOP_TICKS = 10
COSTS = (2.0, 4.0)
RR_LEVELS = (3.5, 4.0)
FROZEN_GEOMETRIES = (
    ('CORE_LB60_M1.50', 60, 1.50, 2.25, 1.50),
    ('FREQUENCY_LB40_M1.75', 40, 1.50, 2.25, 1.75),
)
ARCHIVED_FIRST = '2004-05-31T20:45:00Z'
ARCHIVED_LAST = '2026-09-24T19:00:00Z'
EXPECTED_CANDLES = 546907
EXPECTED_MID_SHA256 = '124e81dc0a8302d45433ed1db66e8df75cab7dc7a1237948bcef357a7b65153e'
# Allow OANDA time for full completed candle; never use candles past archive.
FETCH_START = '2002-05-06T20:00:00Z'
FETCH_END = '2026-09-24T19:15:00Z'
OUTPUT = Path(os.getenv('AUDJPY_SHORT_PASS7_OUTPUT_DIR','/tmp/audjpy_short_pass7'))
BUNDLE = OUTPUT / 'AUDJPY_M15_SHORT_PASS7_INDEPENDENT_CONFIRMATION_RESULTS.zip'
FROZEN_RAW_SHA256 = '0a3f5c6ed076da40b90cfed4ae78d0f052c3ceb1d7a9e2908e10128849d26c6a'
FROZEN_B85 = '''c-q~aTXSSNa^Js8KWi$0ydZ&nlND>f*p|F%KiCRI2QS4@hi0cIdU|yUg}=KKr;6k`<U!%qT?ME)Q?turAM*T}2gpoh=I=i~+&w%TfBSZP{PfH5@#o{$-#`4}AO7RR-S5Bu<<I)Lr_aCq^69tFzv*W`z`=a@s}ILtzkU7pPv1WO`S|HC-yY(pjp3gR{AB${OMf8#gE9X%e%-%){`TqfuaC!n)2~4X=J|79k9WU+`Bgvp_aFcE@18%IxE{Y=@8{!}KOgm5>sS2E-~Q>3pZ@!Qapu$ium_G8{-@8sfBW+F^TXXQ%TKId<MCiv-}!ocI(|L=`f&WT{NU$zWO?WJzx<*<j(+MVyWHB>_`mTG|Jv`LfBsc3_WJW%7eD*$%WtPEK+ymD?e6Q($8Yfypr2U%;Sc}suNpMupRE08B%xtol6M+@{Odpcw?BSNs;F0VAC36~nno4jV9n}}-B(n<3bu+y^;dr{uw2wPfY3@`IN;mw`l_`(kY8!-8?<&m#edYk;8mG7_8Oe4Fxb-t!$)iNYK>C!AtkTAgVhG>Wo@y1K-{Q^hu~KdLz{}C|3r&5`zi*5uF7PWOIj0k#C9D6r*sVR)=Vv|?3BZ0QGHnK{8?0Yupw!ttZ%OVxCVh$UDAI(0w$IO7Vo6OOived8@sQRH^{*<<yM6FRm&4yW3w!>>=I{r^%|s~*q1k`_Gnu*wfxcgl%}4!T2m`g)7HHzgahHe&BnKwYlZY`sjoun3VtxKl`m?GEAwnqQXf~{uv}shbykGylw2;@VV%~fHsEsRxqKP9N?G-O*sf)@2iVFQl_o@9efxS{ecZOuq&Fuoz)-cU`bAmzX#5{6w<>E6r(fruocdniw@WJ+3f3D0U8MqEb^yFs$6IZU-&kzWZy|lkhDBL5u<$CwMp<jY`5H$xJe#a(-uJ~F7$Ik|@v9gr4L05mtSnYUT?|RAs6vMT#BH+5B5TTEt$tInb+W3-Vzk$0S^dF7)^m+tMb)ah;8(UP;lM;GRowxF`@B_EBP<I7rt}31rc&S26{SbtkoN=PS5^A~dOOz3MqmiAQB@A!*)H$ZnkB~6H8CU{0gJ{{g{tQ(y66wLuCmdVg5Q@i9GvyFQlfFRS^>Q7PfJ{|Hc)4&%Ru9eV|-j~$`i1<z|}Za=POc7C^W|b=3spypfU$I8*Gh~Hn^~nl4&cY8hb$M$pTor+&IK%J+VbaFBvMNe7Rz1&!xc2Q@j)Uhfw{1vuJ$1&8P6&go<&Fdi>M#jnjo?Gy2I@j@0RzcG>Tn?NTKnn@y?cS2_FEqe2r9z@WPK9+&6(qR4(71$ffUAKzqBf&|m!QmG*N5BQHJX|EzwllyzUqHng>xak^5`B4bfew1je<X0P#JBV{{oF{QDOD8PRqlx<M=TL0)ovV^4uh*1zBc%gUc6X0o<>*~TC7{<xfvwR1z_Gt9t=^vX8!L6_-(;%DaI>`PMB3B_?*+j&NUK{sx99ojK#o&-l7AmYk$3vYvklCcQ&UbG+pC%$FqNT->xu2kP^ljAVYShG_t#i3AW*aOzg$t;w=2DC6H*F$2$he2ya#JG$y6gQt!i-ex^q#3s(5~YHK5vG6x82WpRGdGmn&jN0zjYke4x5~S#_kk+z_qffBi+tHnjp8Cx<pQB_`-}LrK`E^;K!qmn-(9QSCwZ#r>3@zUAr#^nRV33aW?Sru@vo#Z+RGfPV2HQ}MmJB;c#grq0)tu06^hnBD$ku5XEZ=RKjf5J(?Sqw%D>tfv~fU3*XAU~MWH?KOs4=lZct<nT5%|3+jW#yTsFd6bXPekK)SHkDFVYN*!nkN2=vRWHKnl{CZvu5W*$a}1E)pOBDiR??R%x^`9dNlYA@=sUG1een)xtLf?J<yA<35a4bhWl8oYw<H3#QYYl;itQnO-~%T@e0|GRhxqXx%qsDu7m;m%v|dk1qbKP)^N$+%CDuvhpn%-T^{7x)uNXdpX+MpMSz?zu);pc()~V`Pw&!}_@XcYKt_yw9J}=LuOGo?q1URS*q?T(&ugNugO##(}_7i&Xt>E8>`1&+!>=ndYS>p8z%5vB)r*r_^#HH#6z&ef6sO%}6v?*nsuG7`Z()paf*-YW!abH+{hu@#`TTdGB>Hg0?aAx;^KDvAs77xbx)ItzlE!VgE^Ky>F7j}PL2M}vDc5!j(+5cI+aGzRW{f_mL^&p~JSu&@<{*ub<cx}G!K(ulCRreq(`;{hXt@#Pe<}7I|tx#35&&w6p;aRQkstai<qpELJXH?@o@H#}51A<GttFU&gT6V5@>KUEO^rGMma^0e^TvHbXfS%+kFr_l8An%e@sKEevjSSFvXG3Pa^)Z{V$y*2O(sgPET%XtFmSr-;3cmg9s)k+G*VyjM6>U!f%aKc^SDdT%58^%GCTSHcy~(LaSh-r#CZ(>17+1rDl)*}65c_h?_N+>@5|_wT#S)0{K8SV3F$;>Am=C$AtlpBWgIF7n5K|}BL7fMo-c><<0=cj%1Txmv+3V?w*mI_88B=GfR;mLoy$3_v^Z&(JSRYS|<36v;{j?l{Zg%fCQmWHIRZ^a>*uHJ)jRi;?tms=*owbVhu-dl7>)vNm1}X=NB}F|3D$5oFA1$X!Rt=N)6{@~mG4xcda%w@<x2hM^cn`A8S9`x*Q0WkJNlp6UxfEr86-8^Lrqt*4ilKcC<#<Mt(hpp1(L}rll8t`+rg}OakD0!Jb*UQ#mtHH;f@i9elF#*%rrm#Xm^_hoY^$XN2HGA%Z;K{k>xA>v<tE9pN%#v-wR(1Bs`hqyE<v+h32L7`Fww7a=3V!M?pU7O`PH9_gD<EY%;tb9R<&&krhMFyRzFJ1v+tJ-i`~@ZY*j|oO|QaWU#`gQ&psR+V5&65QFY%$ya%^wMG3~e@#_VEx(cS28@CrqQxJ&_RT=E*n%vdxL+PlL-bbaV&oaM!IG<^rKq6;0OTj6a+MM)@>5EJKIgw&aYAJ{gDAy``O3v7JGgbLkW#Q?13iGzuU+oVuH~>eMO<3xcE{LqIscc2I;cYC%&U5cdAWH%yEwX-Kt;l*!iodvyGBuHsW;J;iRauRqiHy^5$w@}xSkJ!60T}akM>9x$#i=JX_=0mlY7|(v)}D&Av1hF%4k=MdYU?WE(Hp{>wKi2q?ad(s+vg|vSX&a?{zA%b^$FXltGH~p@tUEhTutxWdskq$r=4iKIOyNR^9RZ0ia@<PA82d3E_tR62V94+EiP_YVk6m<p1#qW+1FU{>59I`W6&7$RAIJTt$waotFdCe2g92fHYB?OV+<Sm+5w^lr;Q$yRvbtwRmG^PX>VbZ5N#=yWHBD9RSmdaW@C1WIi;$PRqfPs0t04QabZ)u|E8H$<HCxg*q19>(^beyAz8@ks#n~A#=A`+>#3{gRZ3Rx)_0w_fQ@|+vD+-=-RefmSYe>&D{@bpRcr?^`9)U03TpE$dOKjZodM|0#WZW1svKPcn`#Coiy`<rRe8xkV2@u*&Zmo6YZuda546n}I0<k3SFbK@&%P9IJocJyN>AQMsoxdqq&!`*YhZ;-q^aXu)oJQ@59=<fcvwX)z1xeG?#KSrRF)b7LI9zrY8C8~v8Gra)A86pCs22ub@tWII;i(=HqZL$n0x(z6JXaCA8#DaMbHTY1&=k=RxelVTR~IB0P(F*{jBRftgEe-qp%C^KU&w78vt0&tU2xeliDX$<p$2zT$YlK^Hk%K=c&fK_oXb}Xe=a{?FWcAWVSs(X#Be-0N-`AdeXr93;R79;+o}jxnfK(a8FR>2nvEKS6yqZw*%I0<JLD7m$9z3Z@Nm<0OH|tdO3}Wh)`>-a89yxoe)6!gy8)gAPIy47!V!1Oxbn3C*D?A_;=?8L7;DWh36uO)ZiM5!8Fzx?CFZ_4eNnor}%!YmA)0Y+DaX{ZE>L3x7AhPq$|XaH_l6wrB7O^oJ#R4q&!`*y<at!>xE>|rLFFwUhV-l<$%s_D_HHpQs3G1>Bi$SYLZfVwzl?xU#_`6XsEANkEctyfj|0H@UCAq*6*tm8oWAM4h1qQ`^^DC)YPZ^0sVgZDeARdw=@PpN^mmX_&VEli<f14Y&RH8*=~U9Fn|oN9adcicv}phIy=*9u@XBhlU1vvo^Vay|LYao`zwQP>0VNW!BRc?FW$qt8X&Hk>81dq-+{VnfujatDOgQ9|0EWw-PCXnD4KP3>VYRZ)?DlieE0C9wIr+#s7EoGQg*#3gLYF3%$$l=W8VkK_fXl!jmxA|2B13I4d)DE^TW>v7bsP4EtaZnSFGDsSYNPRJP5k(J?x~x1yxVAPPMixU0oRblGxvC6q5IK>>h0Jfk8Jf4w4fYfVXAr0BMoUdhnWS68WY6JhecC)33eUbkA3$F84K2gcaYaF2Y*w0k`g}v*{L$5TMU=@jPrXofLG2T%GBjuE||pu0fy5M7*VMy^psxlT$M@LiCo9SEno?&dh-~y@NomFF$Z<KBtX!y;0VJuP7QkUs1Y>27^YH`3V>LRjzu~b=ErCs%kLZ<!V6s99dl4&I=t{<T#+OHQlAQy~A{MAw1Ws>^2g<FM5mRz$~Is0~qfhv+O=+6ISCww^Ny;n_#4?WB~9ykoJPKVtlo<a-Q_u(U@hEeu8!HM~HxDbsyquCJ1vbY??LsK*`f=U6VI4Sj)0Y&jP0!i*+9eSd%<lU9o+Ps9)y!><_rm_v#O};$4{afD0c}<_TfryKzWCud-oD_HoK|gV>th_m?Yn)DQ)KIMs;mSgt=f8>)5HSZKY2`9pB2wKgsh>o((-1>w{a9Af`}Vt%j*7VEM!%Qa6CU&n%AOt!!<eighQFvMV-?pZAQ=J~uKJzLlWgS1&7Fx?u92j4VP6qW|zSVNE+Dah&zVoiAVd`0Wp!?Rejnyn#-UlnT471=Ct>^4o~-ezmP?JD7p8ptyI8k4GOEF-nXD{&-NM@d$k2$1v>;C<DxdKLD(qveu{dP8>IfDo4eqBp|_cs(6Mnski^)gkJ-zlkxRm0Df(ns$4f-Y~P-B?`Ki_uf}@TaPL)2g?*tD6<v-8lml=@xbrKHg9Wk8`X`;E0b1NLfu+$zG6pfygyjXuLb&5T)h^=+cBFx4MKRckvVRe<pgfoz9fBC%ZP$CvDot!dq#^{I8XT9u~U5yZS4`Q-h~UR6B}#s#L!r|SaG1Z?>k65b*F~(Kyi*o-&K7ar)$2M_FS!}?z^0r)gIrI_v0}HUOrIF<$fd9Kugw>@Ga@XkctxGZ%|k4k~oZJBMfb*KjYDcXgb=E@uh^Jc+6v8KTM{w;D-=~XfDDK9a2VmQ^F8UNEj-3@ZZyh;NXquzJ$vwoMts$8#hGL;f9cgWDL=Cj3G-oqrEw9h~~o$88<3ph-PFAaRp-tVoCc;tGu{Oy!ss>Lo^>`$ikpxA)1dYG&)R(=7R|(cKMqVglIm3kVtNyzd1~Z=7R}2lXdxT%@v~gxI#QASBPfh3PBA_NaHF??(`Ce3SNySMAOlPz>h=|q8VvIK*b0lee}U;7{mdP<H7WgB?!@s1R<&+2pK?c0xz$0u4lV8Lx`qh2*m|2x8V<j6QT)mLWz*{RtzDUk0As@F@$JFhR|lrQ3Xy&7s}PZ1il4Mh~~oy8O-LO2O<m6bYvm+*^{YTQ-)|h%1}P`9|<@_lL3d^A%sDJLo_3Bh--jDHa^-$f7JLN4p##X(Tu<$lV4ol!-wME0|aJoF>suPatrtn%?KZ2DOh)RI7Bwi91-xyXwSWeSHTd`d>A6mI%FdeiD*V55!MiibU`&@BB~UvBe?~Th~@(l0T<+IZp$X38QDaX&w=e?id3~&p{}+Zhn2xCFhw*UrU(bc6w!Q`qIFg}3{Nbg`G`d@60wNpBNoA+#3GuHSY%B>7shSjMKmM4C^67|k609=UU_cH1VNOm5sPR(Vv!k?SVS`tiwXkR9e5GM&RDcIfOB#8VZ0i>h-Rc0S*}19Q6RGVvx*0MKFhfZvWR9x7I7)2D1OGh9y*=^)9a9mXg*RAj!!D08A(N{QRTaMBKFoHI7Z{F%?vC5L3kpX4^L#r#}m<fcp|a|Rq?}+ifBGk5e!NyqUlIQ&(MeA@I*8po`?p;6VZHlA{-x2MDyW^hzt6PZc8en`A9|i%7KxfMKmL{h)T{}-|>qygs>1Krb+-E3((aNMl>J7C{f>ib8ZpM$Stzv+#=m@@O4&;m8(&UXg+EY=f{fN8el~80gUoZ+&5<#(S$4`@5=#3F{Q=2{ha>A6Uyf503(_XVC4N!03(_YVB}muo$)X%Bbtz91R<La+`}40@AdUVgl<7KqWP#sf(3=#!vKzGM!*r4)XML$jzFJsb3Uhmb0X*3up^oeb_7!5^^U?lq8YhIHoHXcBaq@$Ft8AdTiFHXt1*yhJ_b^1F2_LNBbpI><nr#tE(6J0Vtw90JjR{+dJH6*k%44G!JhY?hh!}B^FQQnLPVkoiAa>)@&<z=(R^?u86S>B)4`GG5Ik29k>aKWv8>#w?-K~GM?|9eh)C?l#30dx7^Ka97D|yw5KHADutssgorCDB5Rqs~A`({+kzxv1d@NXVKK625kB3C_@sO+=34%luLXadIH4MN*qA7VuSiwVrgIE6(jjpI9#F@h7ABIDM*>FhQf<QW%Q#As~t~=AIO~V`l$@w+}QdiA~eG4Wk!Xc?;uK${G?xMbRIHYy*z6F#vCJsqmf7^mXN*HVwhvcyjgjj|{3T-$f=xJsST#7(aGxhlLhY&7{T1P-ySAn%4lZPcBaTZjcgCXt8TLnYn1?JwU>3}e#gg=lYAnglTLO@c}Eh$xk`3Ojm<)CI^NV~F@!H{rWHr`^PB`_p!n_x(mJCrpCkqQ`6tilJ2>U|26HDoVpTf#ze>prFyWrt-UA>=GJheg^IwHAvMW3nby$D|@b)^W`sk`kiUA(G6xr$O5E9TSn1cLQ>?q<w))Xh~wA%XuA_mK1ywB57aB5=4^q+!alYi%3e;*5<fK2`LM>NV?>7B>~3eB1sz;X<y1b7s)OFwpRTv!;ns+y=q}dT@&Eq$PlvGG|Da)NxZkn91y^uc$bql&qWf(zO{2yE)tqHF4De~C0wL8-IDcE79o-xG$E2~S3ez6c6;BWCB^2Y;2j1HTg5|o9b~kxatSU8oNtykhf9K1TvAUHxU0b>2^c2^gEh{wwK+ZOa7nr~cX_A7;*z{+V<hdWTFXfC)aeI|%Sg(Hs##ppzQjeiB=Buwj!8?RHd<0bzXDnk`OfXCh?^8_8#l>!O@~a>{p`b%d_;wy_v8bdN0_9Qt%M~J+LEprm=BheD9+9ik`k)c5R&{MSnfJZ8J3XbYzq}>U(*sQ60HYRHVQc=6)Dw9n?WJ%OIe9R@@qO{qpAa<kYd8P4TY3YwE%@=LHahS5)_hDjxFV9B65d`sw*!wC?xTA=@;=XB9SkjpA?~xOmDPWghHweskq-UjgTWO#UEiTYWH!9HAuwe%H@En4u52~Of+cIcvSpR%6fO1M?Nm0XoOkRwMWXRTmU_iHAF$0q#YD`l*+DVNJslpR+5hFnz_*utCo<C#A5^Lh~sW(+Y11JYe+{i7*LZ9KIVl8G6q|PI*OwUidd8|AL@u)6W=JIY60J9)4cE7q#TuR<Z7TsUVln94uv5;VwaVzf*Selc&J8u9TRHAZBV0xss&IZ-PxM-8<b>}7y*~V7bWzo!x!P&fo(~x42v(KHhj^3nQ95XXffTML%?B4M$|?!+Ly9~WCX^y={F|62yGLyD4|~gv&gJDC9X}%0%nnIWfnD`Liq#x>~YB+z9<fd<T$?V^eUb+Di`3396?w0<hb}EXu}unOId<1;&th?Z<R8OFXC2wQOB`qG8Elm7GVVIynHxIl99H6S%hWWUFc*Mg*IjpbTNyN3koXU;fqugu_jhY00v_&XRC_nMYJYvG_C{%^dfGf7ulY*;7v1gA;~DF1RaRg_?bwb0HTb@S(1@eKiQQ`1%UZTM%+d+N=aEoG7|B<%R>Rl$eSjT(YpS%fn?-ef$6@38eu$dei;FY=JGK?E!1d(L2udVhJ_ko8`LPFY8}*QU31lx$Q~AIlt6UmNJa@&>qthpZntb$amOVYQ5(r<U)2(lkwe#Ipu>`k)RD?AvKgq+uBx?AqYWZBv<W;W-^jJ`jrLV7;Ts8Hx0GY@jkt|(l#sHHZ^XVYCo?YJ$hjt{(Y~rBP$O8gf7%w)L7_&8dVn0sC?RDX$q2BojIEGl<XTBaJ&6M6it$BpfQ1uro&y^-N63g=KrgaE`X2pZ=|uzu5tgeiy)FV7k)^JrOBu*0m{yRH?E&$XgN!gXyNfm84GIRZl99FmW#px=tZiJBQDWXgj$f3JvVdO{6OFx6j>#_yz6n~CkTMG`dV~DqRw=X4B5s2gQ4h2zo;KwM0PXOLFcu3r(il394iF=`XTO49v<!&%-L^X<zbKeCe$l?FCHx}YNZRxp6JO+|iCL7;uZ~$n>%oB7rfQy96rhP&v>q&04;8=!z;F*#gz?bSIt-`+x_G>qQMry(6gzi%s;<jGI-VL-_B|y8v3G@i1wALWQ)}cNsqVT|I?1VALC=A00X8+MJSaUUQ8Sc7<|I_DL*_{6E5I&9=7ct6j_<*_NGdYe<>bifkB3ahKoKX$U|+|;xSX7V;S3pMPD0fhWR8#1$oqV*B4iG?A#-}Vv)wXYvYeA6;=ns(Bqz>D&UOnpIS{bhcJrK^x?b%yC#PdevsBK;5lf(<)ag}xM%NlPPMm<-H=uQ3HcsLgCJV&b*R>jmgKIVqwJAF$5GS+&aT2=L0dau(hP9Ldai|f96MCArjmZ|r?Xz(Nh^+=-8dEr3BrZF=wQL;6(6_w}OUaS!Id~43lM*-&%(1xo=G3O@puik}Qb>*l#Ej4Tnq|lo&lX{F2(a(urVNwg+b}upIh+NBBNdpOm}%5)PQwojgpxTGE92z2bwLNTiaR1F2lHz|2APwRHIK}RIhA@T$0g<jXv5^}D_DfdA?%}J42{X*3QSHsiG2MKdwHx=JtoI<ACGWUOb%orZ#PBe&~(Tg<I6EQ>XE*FWV$<k2qlN+qU6vaWurHx<j{naoPy_mkC+p^CEb>mSL-q<nID79q3Mu0NJDaRXgW^Lnh6}nMCQ<Z$Q<KF<>b(eoE)y=<h(X|TZTZ_;^feLoE!^d!f|LmIL_!i9GZ`Zli1^L4#A=MAUGnqUH;}g9GZ`Z<4o4!zcm_%=0oG~NN60I5sd>iJRH{B59gN?$l)q99GZ@X1Ab5%4$Vlz0jfaYphI2Y_3wBxr;gPhNWh^P2{=?kz|m#N$C}pV)%aTgaA-OJj(DHl?FVAv(1a|UL^ygQ01nLuz=1&lI5Z;w2RZw^@3L?l!)x6&$}LzpG#?AcU^Wju5Eh4~!{V^do=)8wkwfz#a`G8~k!U$I87;>hLKu{mLo?EHxQ3R)-o`cK^dIzBqvg<yv>cOPT=&R1x;gncFVHd@L@&1>=g^Gg9F~H0cL$>5oVpN}R7y3*^EBhDKy+w6hz@5xvXOW?G$T(3Yj`?hoDg|^<dH4IEpR$CA5I6jAZv45vJTBi)}eehY!|ZQC^B{;c$)yN;agC4Xg<mgjzrm^`6xT<T{;X8+@bk^J1{74hvozBz(~Lynh&^RO+o*~ZP`0CBYP(?jC>!s^Eza}lFdve`<}cVnvb_*2IcM0jJ%zKsC5^;V?40)8VMo>^!94x9h#B6W4Rc&6G0rhZCTdmm-NM754b}!0(ZC^vIEx3bN7ZnPa(MqWQXR1?BMv29hwoclNznPL)M8?oC62z(MgPha!$dILF>?bXdOE~T8HLC>yRxdi64ftL-SE~U{J~qO-I>zrWA~f)}i^(Iy5L+hvq};;P_}Anh&i*T+m;1Tgnd2N7>2O4vYlcp&5ZYRC4IL!`=aOjx2zUEE5;5#@?a%*gJ_*@0$a6Xhz_UtpV;>ic>64uO@>w{+_%Ynvb```QakBM(@yk=$(8s_suChG$Dn@`x^94+`XUw1qZhv@6dGQ9q&gX@6deY9p?(_jE8~n(1Z{k2-&>gK92|J^}~i7ZR5xAcxXl*50=z&@1S{NG3?8U0^I`4L-T=oKuSEaQ5ZcmBcsP=7m$5i534~t-IZBAR|J1GS`W=f>q!ld7zoirGa`Cie(&3*^{|Ft7<Uj9aybm;cZBuOjIbUX3iiAA;2z@e`i}v+1-XYNB==Bu#~TddL-Rp=WPFGZO$YIzL-1Td?jaFx)K69i!n#Za;|~J&(0t$?c4N|dXhK@gW@iiK<R0ryEKYQ4x_~0rBlpmh<Q}df_aK2a3=ZQw$Kb9<?xFd}J=P6`>!As8J(3L|1|av)l;j?)BKIIboG1}I8jj;7ZiDWDQP4gA^-q8NU;q1$|MPGDk4jC8)Qe>M{2p#$_Gl2?!tB{FL%2=I9J7a{joD*%4YpiLIM}y5ol=O;s$kXxmWv|SQG7Oi>kV|Tt5bZ+_137swxE0xrkh3itb^Saf?pZR$F-q+Y^U+^A#gdv$HK~DIb2q@4&k$Ls$HAPemOZv)-l*nh449D(59*z%QcpRPc63k7I3A>>V3TFBDKhGrpZVH9`WiBq+$hQ%*9!9eGZDWD`piGNlh4StFTvuB9#jpok?!OA?>SKf<q#~E;{nnaY)5-E?6DNVo|erq+M;x@JNKn1>+(vi5sJLNe8WQ7FwOSrwjJ(`_|w=1oPs?F@LpgfY{UY-PI6faBqhwTYXF$6fFTxF*E_E5=Xcdn#Q)xD(Vy%3eSnGfR|m?66%x$=`+}+`w(n;l7x+C$W+JmS+1Cv=~=8U`<K3@x8laPs#kz`BW`7-9O7ifwVq4|Tt6bvrss7jUd4LGZa9S80+6G6?Mqup^$I}TrtNjHUgg>@sGY2~mqA|%Y3o2=DBKshULEvRBXY?5u{rS9zRD%wFTbu;YFHbt5B@5bR;?=dCgj(?o+ZdHx2B5vHa)M4{3;JxW2KdCf_)|QEP#E5h-d1^5?&wnRg)!reyD4SU;BFIiC-#{bJP=Q6~VvILj(M4@do-VRCOVn&AaZxz|e%(lLN9jrSmkfuq`8RQks?{nN;q@Ixx=GS?)4rE-_KETxTB#MrL)6<tx=*S1iPHTP#SpP?KnRwLbVZmBVs;lIkV30;eMDDtivGuBLazl}M@P)DHHXrdb=Y6>m`-$7%4HNGz60Yz9k&Lonej7qrv!x<1mbCUij4#<ff6S-`au*CJvO%&zXZ;Psl)x#ntoNm&PWA8w~lwpR;dYrxQXqEA}i61p83Zq_$Px3jHuJJYEzxf;YBkaxO`u?@_aleCU-hwJ7L*Ch0n33oLzd(+0Y+m*JKZ5JF-L4_<_!doqISkhYn<H1+5?O<u#>&((Ftz|CRPi9@Uv>w(&yhA+Og11~Cpso{z>$3tYGPD%C@bX#j3_EaN+#+@$W22<kg$I_Ks6CrdJwY&`VgW(W#ly5F74zJ{N;1L=H?Y?k@UfY*f*7dbmW>mJ92p4UE{j@83|u!1Uue~ZafpF6_G%KHU^!M`LfRTuV2F93K1TqT8Jv<XG6ry`5zyt*zFe_yDTsr+Qmy)~+6{=eVK?pNVS7Fk=~(N@>C&L))ya$HYU(m)Tab(UYL*}u1-Q>PuZ~<SSCb=~t_ilduV*D}al=(>*}F;zfu|P9T0)?S3G*(uDiY141`~)zjZtIYAl{0_%SxAE0>O3qtVNhW>)J4ZwzIBC*EuZb1F9S9NuzG{4cG_Hcm`E`!03y)KF{C=e88GT!l&$bmkfwxJ&34amdq)|a!Kj}GGLIEB}<#Wr*4E)jXbz;nzCC?20UNUcW-EfF14c(R|m6tBkM|ey~q|wrQQ~S^qtXO9kf_e2y0SjU>U^XzMPeaMJPj6Ko?>W*)<>*O+2RQJq`n|;VhztVdOO?L+70hnJujfw7673_W5O32QAhl4;{HC#9~6)0>q*O>5Cd}fLJV7G>(w1fh!_9<Cq0SA_(WQyj5()pq`pg?;>$swqnf*qvbZhVnW&iz@j+r^K-AxS}fO7G`@!>%3?y#I?5u&THGE}y*e7OrY{MojSslrj8wu0L^6HlGFL|h)_4aFZ4)Fgp=SXkP=8I-<pjtxzbc8c)3~wfSmx!5o%I9@y3ZCA#dudQt)~rkDO_k$R+l%}&~3E^C`QwYV(dO+@Fm4ZJ7`AT>pZ#h>$$J|g6aiqMp=WIT2cvDXEWB+Cpu^YH16wJ0%#O~zIv`QKqIvR8hdMHlF{xynvwYB!}&~eZ3&u@)<+H8CT{_nk=xLWw$or$L*#-|q#aaaoM!C+9N7qzgUd>nkQ&)}?p+uPNR8M=YBb%Y%OWKM4&I|1t%m4C#>I&|l94&fZB+G;`f_7e=Qh@eOxy-<OzBw#Z;YgeE?=z(-bkSd-q>6H8Q~M&6<F{d%u)X)o<B$~mx|Z29M{vXn_Rc6vm9$$olXUt^+3mjv~@s73Z1pg3w|-$bJDe7{axEyprhH&0%}b=xOCdVL$$R2R8RaWExhd<i^Xq^8eS8MS(6GgDGf6?%!Gz@I80g}nqix?r%r~{DGx>pUtMQTIMWIetlV;OQ5VvLxcHJXTe}Go2Xz927}X`?C}_}N*4C>2*p@)3-^26<7U8F*ru%~DD?;ci<vm}sx79ILgeF=?V-=`<M<ypUj-3YaXe3i59`9?78<SX(?%&9(>t0vN;rcMwszwoNBf9SESweIb@Lfmf*C)DG^^uyq>y;tA?$1H0B)rBRsU~T!i*>DQ7HMO<Cgw^Nuw8v;w(<IG*Qy*jw*g)gde#A6#Y*=oa9zM_%__joHKASir7c0bZiXZKCOxl=cCFDfRrZyGx+e6jgSw)1U75u~sB1MI?-}aaTi{`wtH8Nhh-hXjnC-zUg1PEvx@tYH4|A<LWhm$+&%j;xr7eNGvh<DeEQ7oH@Szdz+JDAdLwGe3NMl3>#AjH|C4CDJuR)-1$-O$_wY(NIj@IHPxNAbs0=O$U=}Hh@AMRRHfp3Cu!n!8(%wk>b!BStvOd;0QerUqF_BJ!A6J)YA3HzAW*yE+nYVhidITXl9UBP+P?-v0ey;gi(&TEYo2h#?7-Iulm_UcTl?OvDmTGKA#yfkrM6DrnmUS(~c*d}eB^9t4OY&b_By&d_O0Ke}OUM*Tn!s@7c6qD(7swKSg8V=Sjb%yXtu0d+)?<(V$ldUqgD|)cO2L>NWp^{U&0`3~UJM1dKzdqcx$`?yihvu-Z32E!Ft|3658-YuN<}SDq%*NH^zwvw$+}+&}bhXG&_|dPz+IAgc3Tu`1BF?z<v@cw@>-aX63*fo#LlZo=cX&cLNGe#{#pZ5kADNMLMc7;**Oz9vIySdPTu&uNd=aCYkhTV+8x97f&-cYMhvfp{mAr>Kl}}fc?%@fL@_efYs>Qq5m@FP>o6*&Em!aQe^GsrFV-k1E>0W9#%4o!~U4%t6bOYlZWW0wG95EZ|DgrCR5&LR=pK-*!t<sjtImAHVS?uzf35Tm7#B|t1!!6zeEeS(+Vd3>4l}U)xHJ1>(&jt3$BxHQ+?Y<W0p{&OL=UiY9r^N6pYfzGtYa)oyN}mgSb=-5sTrj`Pmuvz(?~7XvdX8h9dJTDbv?0}hMgc5IwY=X0jG?PBCe(F@`fRV>h}{52<lwh;AkW~EMj0Voh$hrVJ=e;JCVI})%NOz9<fqq}L#S31PaUgk9!~OjJ>pz_Xw&w(Eb^L_mO~qmJRxWukbF}E*0Q)30m-ouNZ#)(7?bTT*$0#ZgK-)ru;`oT<Ki?itCCReo4_NtfN9sz-2lod+OcEDo}IJYNktJvo7XeMq)FToh<2^GXNdN9Sb?2Xk+iKx)tG~2U1NnZ_FII~e$zkHDDgE>+BM7Y26b<n(Ap_&^Js0yErUg`&(<!t*a+AT&_>nX*RhDIEy4Gt8*f0>t`JqX4z5CL>xS_1fnqMVPuHWh!+L15tB05mjn>}auE)b{)0~)X<4e)n@jThSewa*c(+@#w(}ZYkI;70?rf6-N6|G&c4!p-~!@(QT4GWi7IE@ayHfNir<ZO!{hqFy{a<-9%glp5BaBWMu1>okKZJLy`ZQQ7EZJHFW%{UZMo2Dgda|KZwVpG`5s48)pe9L!4YSW}hZ8IoRn<hnSTNsq1O_Or8M@MJVr0DE~YkG5DHciUQ7RfoMH%DjFr08sCvVs4tDcUqCMVkkuXw$S5ZKy$KThyS|onGP?_N%e8X;OA};%s{)U^dMP%zkdWEdyrjmg9{&P(tb)db}Dio8|;&gC7Z)P16Fifr=^Fx`%-iwj4m?QfKXtg=EvTkZh`fWE((l0xz$0Zuz)2CY$EOWXJ70=g1C3%%(|+*)k|Gn`R|uCvsZ1!erB=m~0pdlTFiNvNwaCD~Q>;8LdXb@hyniG$}FLVD{v7Ab2)S3eTo&fy=Ey+B7Ffn|)r{8$-2eQmA%5H#-tvo2KP!a}8hHM$0t%^Tz*hxEf!ZrsZp!`~vZwwH^N+ATWE2f#Vp+Td=liTGlp8!P>Ed-L`SkqJT$6d!A2o73wxkO5L_&QnzVR>NaPc;gJB`G%bJ|YXIE3XBjc*b?dV^>ctP@Z_}jwZQz2e<ZV&hG%bpo@+se4Dz{1(n-SHj<2-D*1(lm7rE=q-RBoD-%Dq053Bv=rX;MHp3<~I`Ndeu~6m*T=meoy@vbte>KsQYb=yp&{<&GH%d7j<@f+$y`a?_+#ZZjyAo2I357evE5fNqF$RM6T0&Kah2x71Y#-83zs+j0e#n*x#5A6Rs`KA+581<Os-V!62##T`H6UJsLPruy-F(zt0-8aIwl<ECk8+$p}+E`poAbqJ2pIBRqK(jSB1rb!Xpc6<akO^V>Afe_p@DT149K?m9}G;W%e#_fGx=N&M&uG0ZuKLD-<=B8=E-1!{NKBHUx12!?UAXbGAd^JcnO$zCTK_T5VC#3rsYdRd6n<gc5)1YK-nv~3q<CD2*QZhGjL9_a8A>A}7q&r_PHWII!rsZ{0NkPavbhkRV7J|eiETCfnx*ENkCZ%_$y4i1y?51gv-L@RrtveCEjs~@IHDEVQ3hd_m_{CeZyJ=E(cYb)#%`x6IE5_UVa&~vjn6vInr+@JTaK1Xbo91M9dp{Jrn<iy<J6F((G7QF>X2p0z$fj}kh~Cl5ef<!jTL8UjTA(+UwEXO_y+O^<oKG&{oTIol+?ytadxMntZKH6$X->{J1R5UhP1C}?ZFa5R=X}S>wO}C@=(7vVR|9?1q@Zu0;pyHqC*6BZ&rQd8H${EZw5V^F_vLn}-_{cAo(1CLfjeD~`c2bPzilY+;NBC!jYWR`hulpV;501*+}1F_NxwIS^9z`pK)`8M2smYZ<-vI1G%F9>4#fkfS$W`YOa?ej$^e(~8Q?T01Dp;a!IRquf5%7xVi%{=&@&KT5ByEDg1<MeStx~oL+ts3z{W?^prxxZz-e9vI9D*h<C&89SQqX*orcL?i2zQM62MdQif)7brdhGyk`0OnAb``n1aPb%fWyJ7yN^a!SrXz*-_;KTfWwpkaBhKqpA4%S`fb<qdZ<mv9Q4~&Wp7@g-~AP5_AQFL2mr1&y8dg%xr-v#0l?So;}_Nmm=FN2eywc*fG12h3jp_6<Q7050KBTW?-c+J-6L`mc$yvPw;HU+mp`ZZA{UgcV}7qoQZFQCU`pn<H*L)CU13X@-<O!RFemhzDYwSv_}{x~R`I`i>6>j7c2xd%!r{t6zxUNFfqv^cP*QaYlS04WP8V(9fA4Br#{b53Zn4c)N9KPE=EcpRz<0$hK!Mvs0PgbyN}%ArDwFsE1-IO5y5=BK0R@juZD3J@C}C2)>_vS`nBZ>R|K4J{V>7{_jS0RhX8{vDI<C~M=fF&Ggxpe)!-MZjTZspcNt-5V2d0BVHlEF(!V}8Yp~B6&?^4=C9vBs#cW86e@O_m_sNrIuYbP6=8XkNTDtuqh5>&YL+?8$_92K7E^~v$U6M7c#!F2`d$`=mK2bVTJ_`aTbKDc_W*e>E_C^$*oP}dU_ynlpt9Gyrun|9shgNyeTnd7)1gW_FI-#i~&82i@KvH9SXb+vM+@O^13QQ-!q%ONO2g`ZkGD^cMw=<waBaI!V1a0$X<Jeydo<HBX7OHkpAzAG;?HYyxV8!9}ZX8|hw&Crs1J&UN}uFhPauW9=yS|DXT*gb%F92yY3!$7P{D}>h}#rsm15X8axW_@!6@fLlTW5w?IOs<9?F1kO6!C-ZpU~6-t))B<j`|MkJhbD-7(}oS-m9`ce?y1w&9UB`S+OXjXX$!F7v8=yI#F1Iy`CL~P8@{h-5jGrro1z0#!>NrLo)EEs8cx2`87Sh1hpNMaSAMwfCv32*fQI|X%SG?W2RJ9j)ACkA!wGFkw?9k@4Nvrv=1AcQX=_N~8sb^*D$*F76fP}v@Px1hba3IK0SPcC9o*R#BKW?VB}8yqtI2I@4on13ja$lK!1wj6#DIJ6`=l+wfJ-F;=G2yByYt(T^({3RaPf9&%l3{O;^kxSA`H0c%@Y)1z$@ce{!Z>|1bkyD`Ws`bn2)G15RJU8T#jPv(BF28vxhdJhem&=?0A>=?c*|lMwmsT0Hlo61;B4vGcBb}-+_VO*arMg=$Qw8gEw9E8-oMCQ-@#~%J;sUm6UJ0X3MrDT}mk5;_Fs|6Xl!ZnY!(4?f|Z#e8(_M*Mi_<zLOxc5>#Qn<HS4e^K!?=e3NTpdncqVV0$~kzEXqH+1{=O=<W4KW#dp7;uCjS-YTHC-wqybwA_J#-rNTCPDon-^i~g|NyL#U-T+PL?u3YS=x$uc4K0bTvC-YshVI^PMk+yfgYj)54$SLD+XU%Oh*$vWHfvn0YtwUJNOwWWQHIx@khYH3Ei2Ab@Xadcc-?iU2`gTAug>gCDvsRy(Q{6>HBaYP@tje;0Nw4@#ZN6!*x2Z9XhV1J>sf;C=5_a%Z`Cu4?ylxZKcl;QiwTl(?GCRSBTxF}!&#Dyz6HGQ&D2_M)3<=vUCZTtNt|`_x{(X2j^3fWRT!}*mJ$I5V=gBai^$!yCd)Q13<c!wYC}GeyKVQ<@MauNA;mkUa9teh=pB(h3q%>EvlQ>xcelzfV6)PrQoOm1;+@j7isCKedsmABinli(S}5MVSlLMN_O8H)-vPZbRuQ}a3D8_V<*5aFL)l<)m=x%ZZ9wmYv~@u5b)90<5yaR)Z`TI&PDq;vdN-}MWAnUI5@sOY2?^^U-kSpZMrjKm-c{4PULoGSJ&maAC=`>sV{*a=9|;~#*q-63X{qz*?h^~Vb73t&cROEYx~D64KmD-^)E3ja^QK$E=k^}D^c<YeO>KPceLYL~+zwsW?ld-^+nT(ul|gmy4?L<wb#M6gp-tt1sok!P+MSpsRY2{Qbsv7Co&!_6xsBSL(6f%(&Au;NJ~*}8xh7QizO*H%ZeZzMU`IxEC+fR$eC~vvb$o8XzWUliK6f=l@0ritomX_O7|I<dAvzJ~Ij~W~gpAS!Ol}*bZzUL;$z9Mvp22hPOIw2HCfhd$sSM8@%!fuicW<$m!*V<~#<@vi4S0is0jy;7EdX?T>8oEJ9MGK@ik72tC-f|!a>rweUOfk<atGgp;!fz9MRDuixONJ-RnIJnyPCPn=gHu`DDHTOog0n1L*>R;yy!^d?>LG|jO3Pr3M%(PHuT+AJT8?xm^Lc+zO*G&Zrx$pL>w5(?WKvtoe;5(#7*m&^w=hCp2QvCLko#}J%PEg^Bz-pyB+*C#;OeKFrcdFV*Nx$>N@^*Y(nhmyD8&uduou{cULRL4m|c1(Cs+SM5BI^ctCEP<fN{EZo{?(hnl1w8M>XQ3C)qV6Vld^wk7oSjEqg%Mw=hzlYwsUt6T})2I`xnSP0#&DqnhqZu@Q)w4{QyUDCF!{&>jr=oOK+4fgfu4Nlso5-Yw4-A+hb1KswKP1fi94h`Lg7S47;*8<KqfpnW}p0mwuobB#mH14HzqYSPs;=ns(Bq#1%Y<fZ80=PB=?AA9A*RC5)vxaN;x@VTkf!bmTJfC01XT+@mYR7r(eTbVPptf%UYWwb!$;fiWa-cRI(D2J2$t2J#aoU#qT24pDX(vvAvkdKhajO~HxW;)?v%JGHw6Teyoe;N(p^XHsGLFm8_9j2G?jlb+p=c#fn{DUm#Mo%<(1zAdXj_NY2I`v%T87r9MznV59=c>qw$)^xvMn0LS_#G>I&~#3r+8{9+q#$b8T8=1ZGck7HXfMq*;%s;nf9q7$~HmYP_{zKw*SyX+2&q^mN_`7plru8B6TSQV^uLqrqH1bv+dT!d(f)zu$XPk7D48S+bM1H#O;^^uh(;M;C6sE%J#mDMU-vAK6>u>lx?n{Y{v?d*AKCm$F<f|wmnnHl1o!kwjtzj@i!%I)11U@<I5@A>R!EmWEy{e2xOZkgly9xWvw@bY}2fe?Sf@s54as2D&4P^SL-tQ$R9)8rYVWr=g}Wy5VvVg;x^Kdm~EO9vu!E20^FRqO_LJ0jT;rSO_O4_83$!+)3j`Du3~Gy4zaQffv&~Yrb*e_W>B^^P0H4`FeXr&CIxDb&d{bw8QKXK_2%GgniQNZl5<jT&d{bw8QRWdGXS^7Y15=QZ5|1yP1EAEp@yN&szErvK)4z~n<gb_Cl0ko!e-N~*zD&v+%jyovvF8oAnTB<_gBMa)126B@PlHrX<BSHPz5U+9jecwf5($Kjpq4*sBD@Rl}$CMY~5siY`|MyjlTsco8}~Ci}zVCb|7drO$yDHk)YW$D>OTi#JUkFn<gb?!=R*WnwFG}oLxhAq1le%wHp-W7SL>(6q;=?d-ysKKbt1yXH&Mw<<=~1nv<o?KCkSJx!N=-S394U9SN~b(;~LH2C>cFMw37N2mRF$+cYg=+vFFBJ>0hLUp}HLEVDuMatquxO^e%RDOfvp$lK1Tt6@p?RI@x|wyr|nrb)@$c1-d%O-kP8tT#Logqx;?aAOUGTZ|JTuL~-&Ww-@{n<iy&0~aJEZ;RumX>r_?&-m_=x#MIX<1Aa<=+GLz1)7^CMRVgwXl|Mm&AoPs!tj)Cnv~KFBT>3(QcAZq1x=#21$NV<z-}0y(oNG+x*b%YxnJk1S+bd7<KL6aO_P$j&7fp%nwHF65clpvx{U{RUSmnbfZkq>(M{7bx-A!jxg(=jT?NbfIFr8k>!G=6S~NG8<G8_kdESP+WC|X?CytvY#c|{KIBuF2$DM*~?J&3_$|-QL9-YJ>D(5o%7zj5_3gNcnL%3;D2se!b;igF;+++*-%!c7{)2uvh@AEqEqPY>`+05xRH&*d`lDTP8GIu_Kb8|>HO$zCTK_T5VC#3tCWjZpMn<gc5)1YK-nv~3q<CD2*QZhGjK{xtsA>A}7q&r^~HWII!rsZ{0N!7;=x*IUolLOcYKz8A3=x&-6-JNP(zcsI$rsZ|p8eX@hIPd@TdhSrU3a*<b#dULj&f=}f-83n=J3k%h<`8e172@rE4Y@n+?a%*$gIj>RX-;sr_alM3X;N^va|P`t!!W#QR)#l(Z2op1=MD7wVMC4^`(tq4G%d~>OZs|t$lkGj|K-dwtND-Mdefv_Z;%qFZ4|^e&58Iv&k-J)>rK;gy=``>-bZ}1t{P6=A=cmlAxE73euQtDl<*BSJl31$#ClsPncr|z#y3sN_;z_uZWs5>Vg;i=ghDP;0`)y<-!v`l+lB(~?LGNh_ovr?4CJ`#KLq_v)1tp^Ir>}OEN{r4UL*+Lg8faivcD<oDG!DLr&%%Jb|?%u&58kc!=t}xQuMcskN&1P(cg3k34Yu@@0&z-GbeGlp>>%Cp&x|%rde^{8+R>~qra^;u~)^Z=zt>ELx0n}=x?q<f9rN+4THl7cL?r!=x>@7{hb<9bQ9h;&C2_hY@|E@`kUrOe`6K;8v){ExZu%n92570+28;8S05g}{LAs{-OtAlfA|mm-+%daA+C`N%W2}%=dZtidu2({nWDC4$o+D+`idys$>Ca6vHku}cfjt`pFcnT_T}@h-#&f$r(Yfx02Cd)eS9!?db99I`isKjqkX)0?&u$%gdgti<?%u8?6S!C`-kHbvfRnNQ%@6*YAhcfKmUCE{hM0IKd9Y2JRE=friSy&@$u*5*H5Q!tKUekX`YTpyw@lCc&|>OdAjEZ@(#kIgh#UY_~_&zIR7BW@MQFR$Np{6i0jea-#_Usyj=U+*Sp8#?@K#N{B)91|MVCA%&&LfKL5+{({FcQKYxqA^S}G+zyJHKDgDjg{^^fuMQ*X8Wti93yMO(3{x!ecz@2{xCisW=1F9HLY9yA}In{z5)%mm!;U3JBJ096Sk$cozx^pVqJ(GHw=Kk4;@NmwBq?Ee9dw4qDqkp=;yN@HU9)*u|cYoA)hV8?>e|mCk%+V0IyQlDIDY(0UclxW72jd?f>Xf31m4ZiycgXhs-rOC{@e!PR!n>n>ngam1d$=RZhK_eGpo+uCJ9s*py9Yj+n1I_?O6Iev_)EN~)T4g6@Z+5z@cn}t6+a?5dU8VeWbr8M-IIa4$A>48d%g=QD?5u$9_8`mHC8A^6DtLGx)>ooseK&p-IG3U*4l6v<mma~-s;k2_3977n#V^z-aj25AMaI0kt3_6Et%){N-cSnZktMMd3$mlpWt4CzgKD8gD$1VdjZlP+Z{m{67_CXA7ObQdeQ~`(Jz^LW97*aI$eB<AHfIn<naOBqkVXwaCdkA@Z^uq9#uwETMI`Yt9aFjbZfDXcMtTWOIa+~eA54APODP6Tr|5}A@DuI$x0nNz0~6qI6xm~qmqfWKsI1BtRs=XgNK;Mx_|HwkF0yaqyNukUD=26j_{~^9a4yo-ksBj&hasjZh=q6(i1*BG2O*IQAj_{cMoukN6QZn_bxaW{&QK^B9}UgzyEf8ID1>kU;6!*$AABH_wB1Xe9_U?l{L5O{_^F^KdbBU&k+Y6|No!w{_WGF-p=EffBp3oZ20BMzkU7u<*Rj{zJC7w7_T0`%gcB8?dz8(_2i%K9@Gn8KI?-)G0u@U7~oVLclYbh$JGy~@y}MeWpfGZhe|!@FW*$tMFu=L@YVsx0r;@QL|Z@SUqAo)>+$O%MlN|Yj81|KRs&$&c0GmDnP{I*7)iLSQ3S^URHxxo4LS{FPbz_3f-4SA12aCQ80vI7{o-GBT7cOakAOaSs@AF3c;n;DXHWV|Lq@BSnxHo)bVR326b>%1iCIOpC&DGK)}T|FJtdacl~|M#OZ2DoHaLNZ;KfVQNn*`1x*Tg{?9|J2=L)<M@d6~*EEkW%Ek?AWUO8;ETQ;Ou?GQU{tXaDd$Ax*)#~{7}a)Wj*E@QET*Xx?&%MCBB9m>Iju8eWe1*!?22D#RY_rneEB{fg%!*W7aD_-!{gg8CPqp?yi-MghtrSs}|>*MO;xi(%>4sK<3@$Bx^?qr#(QT@n*VGce54%IWje(A^`NY@WU@q*#he{@N@fBb*{A5f*l^Z'''
STATUS = dict(state='idle', progress=0, message='Not started',
              orders_supported=False, trading_enabled=False,
              portfolio28_unchanged=True)
STATUS_LOCK = threading.Lock()


def update(state, progress, message, **details):
    with STATUS_LOCK:
        STATUS.update(state=state, progress=progress, message=message, **details)


def when(text):
    return dt.datetime.fromisoformat(text.replace('Z','+00:00')).astimezone(dt.timezone.utc)


def iso(t):
    return t.astimezone(dt.timezone.utc).isoformat().replace('+00:00','Z')


def save_csv(name, rows):
    rows = list(rows)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    columns = list(dict.fromkeys(key for row in rows for key in row)) or ['empty']
    with (OUTPUT / (name+'.csv')).open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f,fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def package():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(BUNDLE,'w',zipfile.ZIP_DEFLATED) as z:
        for p in sorted(OUTPUT.glob('*.csv')):z.write(p,arcname=p.name)


def archival_reference():
    raw=zlib.decompress(base64.b85decode(FROZEN_B85))
    if hashlib.sha256(raw).hexdigest()!=FROZEN_RAW_SHA256:
        raise RuntimeError('Embedded frozen Pass6 ledger/archive is corrupted')
    obj=json.loads(raw)
    if (len(obj['specs'])!=8 or len(obj['accepted_ledgers'])!=674 or
        len(obj['digests'])!=8):
        raise RuntimeError('Unexpected frozen Pass6 reference counts')
    return obj


def fetch_chunk(start, end):
    if not TOKEN:
        raise RuntimeError('OANDA_TOKEN is required for read-only MID candles')
    q={'price':'M','granularity':'M15','smooth':'false',
       'from':iso(start),'to':iso(end),'includeFirst':'true'}
    res=requests.get(f'{API}/v3/instruments/{PAIR}/candles',params=q,
                     headers={'Authorization':'Bearer '+TOKEN.strip()}, timeout=80)
    if res.status_code>=400:
        detail=res.text[:380]
        if res.status_code in (400,404) and any(s in detail.lower() for s in
                                          ('no candle','no data')):
            return []
        raise RuntimeError(f'Historical candle HTTP {res.status_code}: {detail[:200]}')
    result=[]
    for item in res.json().get('candles',[]):
        if not item.get('complete'): continue
        mid=item['mid']
        result.append((when(item['time']),float(mid['o']),float(mid['h']),
                       float(mid['l']),float(mid['c'])))
    return result


def fetch_archived_candles():
    all_bars={};cursor=when(FETCH_START);end=when(FETCH_END);parts=0
    while cursor<end:
        nxt=min(cursor+dt.timedelta(days=35),end)
        for bar in fetch_chunk(cursor,nxt):
            if bar[0]<=when(ARCHIVED_LAST):all_bars[bar[0]]=bar
        cursor=nxt;parts+=1
        if parts%8==1:
            update('fetching', min(10+parts//3,44),
                   f'OANDA M15 candle chunk {parts}; through {iso(cursor)}')
        if parts%12==0:time.sleep(0.04)
    bars=[all_bars[t] for t in sorted(all_bars)]
    if not bars or (len(bars)!=EXPECTED_CANDLES or
                    iso(bars[0][0])!=ARCHIVED_FIRST or
                    iso(bars[-1][0])!=ARCHIVED_LAST):
        raise RuntimeError('OANDA history coverage changed; fail-closed. '
             f'Got {len(bars)} bars, first={iso(bars[0][0]) if bars else None}, '
             f'last={iso(bars[-1][0]) if bars else None}')
    h=hashlib.sha256()
    for b in bars:
        h.update((iso(b[0])+'|'+repr(b[1])+'|'+repr(b[2])+'|'+
                  repr(b[3])+'|'+repr(b[4])+'\n').encode())
    sha=h.hexdigest()
    if sha!=EXPECTED_MID_SHA256:
        raise RuntimeError('OANDA MID source SHA256 differs from frozen Pass6; '
                           'NO research interpretation until reconciled')
    return bars,sha


def atr_wilder(bars):
    """Independent native-price ATR14, first TR high-low and SMA seed at 13."""
    n=len(bars)
    high=np.fromiter((b[2] for b in bars),float,count=n)
    low=np.fromiter((b[3] for b in bars),float,count=n)
    close=np.fromiter((b[4] for b in bars),float,count=n)
    true_range=np.maximum(high-low,
                np.maximum(abs(high-np.r_[close[0],close[:-1]]),
                           abs(low-np.r_[close[0],close[:-1]])))
    atr=np.full(n,np.nan)
    atr[13]=float(np.mean(true_range[:14]))
    for i in range(14,n):atr[i]=(atr[i-1]*13+true_range[i])/14
    return atr


def previous_high(bars, length):
    """Native price, signal candle EXCLUDED, high strictly > historic high."""
    high=np.fromiter((b[2] for b in bars),float,count=len(bars))
    prev=np.full(len(bars),np.nan)
    queue=deque()
    for i in range(len(bars)):
        if i>0:
            j=i-1
            while queue and high[queue[-1]]<=high[j]:queue.pop()
            queue.append(j)
        while queue and queue[0]<i-length:queue.popleft()
        if i>=length:prev[i]=high[queue[0]]
    return prev


def independent_signals(bars, atr, lb, body_min, range_min, prior_rise_min):
    """Direct SELL predicates; no mirrored signal engine or Pass6 import."""
    n=len(bars)
    op=np.fromiter((b[1] for b in bars),float,count=n)
    high=np.fromiter((b[2] for b in bars),float,count=n)
    low=np.fromiter((b[3] for b in bars),float,count=n)
    close=np.fromiter((b[4] for b in bars),float,count=n)
    prevhi=previous_high(bars,lb)
    prior_rise=np.full(n,np.nan)
    prior_rise[17:]=(close[16:-1]-close[:-17])/atr[17:]
    good=np.isfinite(atr)&(atr>0)&np.isfinite(prevhi)&np.isfinite(prior_rise)
    mask=(good&(close<op)&(high>prevhi)&(close<prevhi)&
          ((op-close)/atr>=body_min)&
          ((high-low)/atr>=range_min)&
          (prior_rise>=prior_rise_min))
    mask[:200]=False
    return np.flatnonzero(mask).tolist()


def simulate_one(bars, index, rr, cost):
    """Native SELL fill-to-stop risk, target anchored to reference price."""
    bar=bars[index]
    reference=bar[4]
    stop=bar[2]+STOP_TICKS*TICK
    reference_risk=stop-reference
    fill=reference-cost*PIP
    cash_risk=stop-fill
    if reference_risk<=0 or cash_risk<=0:
        return None
    target=reference-rr*reference_risk
    for j in range(index+1,len(bars)):
        b=bars[j]
        hit_stop=b[2]>=stop
        hit_target=b[3]<=target
        if not (hit_stop or hit_target):continue
        if hit_stop and hit_target:
            reason='TARGET' if abs(b[3]-b[1])<abs(b[2]-b[1]) else 'STOP'
        else:reason='STOP' if hit_stop else 'TARGET'
        price=stop if reason=='STOP' else target
        return dict(signal_index=index, exit_index=j,
             entry_time_utc=iso(bar[0]),exit_time_utc=iso(b[0]),
             reference_entry=reference,historical_fill=fill,
             stop=stop,target=target,
             result_r=(fill-price)/cash_risk,exit_reason=reason)
    return None


def chronological_p0(bars, indices, rr, cost):
    trades=[];k=0
    while k<len(indices):
        row=simulate_one(bars,indices[k],rr,cost)
        if row is None:
            k+=1;continue
        trades.append(row)
        k=bisect.bisect_left(indices,row['exit_index'],lo=k+1)
    return trades


def verify_ledger(reference,actual,description):
    columns=('signal_index','exit_index','entry_time_utc','exit_time_utc',
             'reference_entry','historical_fill','stop','target',
             'result_r','exit_reason')
    if len(reference)!=len(actual):
        raise RuntimeError(f'{description}: accepted count mismatch: '
                           f'{len(actual)} != {len(reference)}')
    for j,(a,b) in enumerate(zip(reference,actual)):
        for col in columns:
            if col in ('signal_index','exit_index'):
                matches=int(a[col])==int(b[col])
            elif col in ('entry_time_utc','exit_time_utc','exit_reason'):
                matches=a[col]==b[col]
            else:matches=math.isclose(float(a[col]),float(b[col]),
                                      rel_tol=0,abs_tol=2e-8)
            if not matches:
                raise RuntimeError(f'{description}: first mismatch at trade '
                                   f'{j+1}, {col}: archived={a[col]}, new={b[col]}')
    return len(columns)*len(reference)


def run():
    try:
        OUTPUT.mkdir(parents=True,exist_ok=True)
        reference=archival_reference()
        update('fetching',5,'Independently retrieving OANDA MID M15 history')
        bars,sha=fetch_archived_candles()
        save_csv('coverage',[dict(source='OANDA MID',candles=len(bars),
            first_utc=iso(bars[0][0]),last_utc=iso(bars[-1][0]),
            sha256_midpoint_ohlc=sha,archive_sha256=FROZEN_RAW_SHA256,
            parity='PASS')])
        update('signals',48,'Independently computing native SELL geometry')
        atr=atr_wilder(bars)
        accepted_audit=[];raw_audit=[];summaries=[];complete_ledgers=[]
        for geometry,lb,body,rng,rise in FROZEN_GEOMETRIES:
            ix=independent_signals(bars,atr,lb,body,rng,rise)
            digest=hashlib.sha256(''.join(iso(bars[i][0])+'\n' for i in ix).encode()).hexdigest()
            frozen_digests=[x for x in reference['digests'] if x['geometry']==geometry]
            if len(frozen_digests)!=4:raise RuntimeError(geometry+' reference rows missing')
            if any(x['raw_signal_sha256']!=digest for x in frozen_digests):
                raise RuntimeError(geometry+' independent raw signal SHA256 mismatch')
            raw_audit.append(dict(geometry=geometry,independent_raw_signals=len(ix),
                                  raw_signal_sha256=digest,parity='PASS'))
            for rr in RR_LEVELS:
                for cost in COSTS:
                    label=f'{geometry}|RR{rr:.2f}|{cost:g}pip'
                    rows=chronological_p0(bars,ix,rr,cost)
                    control=[x for x in reference['specs'] if
                              x['geometry']==geometry and float(x['rr'])==rr and
                              float(x['assumed_fill_pips'])==cost]
                    archived=[x for x in reference['accepted_ledgers'] if
                              x['geometry']==geometry and float(x['rr'])==rr and
                              float(x['assumed_fill_pips'])==cost]
                    if len(control)!=1 or len(rows)!=int(control[0]['trades']):
                        raise RuntimeError(label+' summary control mismatch')
                    fields=verify_ledger(archived,rows,label)
                    total=sum(x['result_r'] for x in rows)
                    if not math.isclose(total,float(control[0]['total_r']),
                                        rel_tol=0,abs_tol=2e-7):
                        raise RuntimeError(label+' total-R control mismatch')
                    winners=sum(x['result_r']>0 for x in rows)
                    if winners!=int(control[0]['winners']):
                        raise RuntimeError(label+' winner count mismatch')
                    accepted_audit.append(dict(geometry=geometry,rr=rr,
                        assumed_fill_pips=cost,raw_signals=len(ix),trades=len(rows),
                        tested_fields=fields,total_r=total,full_field_parity='PASS'))
                    summaries.append(dict(geometry=geometry,rr=rr,
                        assumed_fill_pips=cost,raw_signals=len(ix),trades=len(rows),
                        winners=winners,total_r=total,
                        reference_archived_total_r=control[0]['total_r'],
                        full_field_parity='PASS',
                        assumption_not_observed_spread=True))
                    complete_ledgers.extend(dict(geometry=geometry,rr=rr,
                        assumed_fill_pips=cost,**row) for row in rows)
                    update('confirming',50+int(45*len(accepted_audit)/8),
                           f'Independent field parity PASS: {label}')
        save_csv('raw_signal_parity',raw_audit)
        save_csv('full_accepted_ledger_parity',accepted_audit)
        save_csv('independent_standalone_summary',summaries)
        save_csv('complete_independent_ledgers',complete_ledgers)
        save_csv('methodology',[
            dict(topic='SOURCE',detail='Frozen 546907 OANDA MID candles, exact source SHA parity; archived through 2026-09-24T19:00Z'),
            dict(topic='ENTRY',detail=str(FROZEN_GEOMETRIES)+' native SELL, prior16 completed M15 rise, no new filter/timing search'),
            dict(topic='RR',detail='Fixed frozen control 3.50 and predeclared interior provisional 4.00; both at assumed 2/4 adverse pips'),
            dict(topic='SCOPE',detail='Independent historical implementation parity only; repeated in-sample data, NOT new unseen OOS'),
            dict(topic='NEXT',detail='Exact Portfolio28->29 requires AUDJPY LONG and SHORT raw chronological signal streams; nonhedging same-pair opposite entries may change incumbent acceptance. Do not simply append short accepted trades.'),
            dict(topic='READ_ONLY',detail='GET historical OANDA candles ONLY; no account trading API, executor/probe, orders or webhooks'),
        ])
        package()
        update('complete',100,'Independent raw signals and ALL 8 complete accepted ledgers matched frozen Pass6',
               result_path='/audjpy-short-pass7/results',
               independent_full_field_parity='PASS',
               portfolio_28_to_29_tested=False)
    except Exception as error:
        save_csv('error_report',[dict(error_type=type(error).__name__,
                        message=str(error),traceback=traceback.format_exc(),
                        orders_supported=False,portfolio28_unchanged=True)])
        package()
        update('error',100,str(error),traceback=traceback.format_exc(),
               independent_full_field_parity='FAIL',portfolio_28_to_29_tested=False)


@app.get('/')
def index():
    return jsonify(service='AUD/JPY M15 SHORT Pass7 independent confirmation',
                   status='/audjpy-short-pass7/status',
                   results='/audjpy-short-pass7/results',
                   orders_supported=False,trading_enabled=False,
                   portfolio28_unchanged=True)


@app.get('/audjpy-short-pass7/status')
def get_status():
    with STATUS_LOCK:return jsonify(dict(STATUS))


@app.get('/audjpy-short-pass7/results')
def get_results():
    if not BUNDLE.is_file():
        return jsonify(status='not_ready',message='Wait for complete or error'),404
    return send_file(BUNDLE.resolve(),as_attachment=True,download_name=BUNDLE.name)


if __name__=='__main__':
    threading.Thread(target=run,daemon=True).start()
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),debug=False,use_reloader=False)
