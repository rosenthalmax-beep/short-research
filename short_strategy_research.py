#!/usr/bin/env python3
"""EUR/AUD H1 LONG #28 — independent, read-only implementation confirmation.

Separate Railway research service ONLY. Never import your live executor/probe.
Rebuilds frozen LB25 sweep signals from OANDA midpoint H1 candles with an
independent scalar signal/equity implementation; exact trade-by-trade parity
at the frozen 2026-09-21 11:00 UTC research cutoff, RR3.5 and RR4.0.

Historical 2-pip adverse fill is an ASSUMPTION, not historical executable
spreads. OANDA live pricing is read-only and is not proof of a live fill.
All historical periods have been seen in research: NO untouched OOS claims.
"""
from __future__ import annotations

import base64
import bisect
import csv
import hashlib
import io
import json
import math
import os
import threading
import time
import traceback
import zipfile
import zlib
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from flask import Flask, jsonify, send_file

# ------------------------------------------------------------
# Locked research contract: no discovery, optimisation or orders
# ------------------------------------------------------------
PAIR = "EUR_AUD"
GRANULARITY = "H1"
RR_VALUES = (3.5, 4.0)
LOOKBACK = 25
ATR_PERIOD = 14
BODY_ATR_MIN = 1.0
LOWER_WICK_BODY_MIN = 0.25
TICK = 0.00001
PIP = 0.0001
STOP_BUFFER_TICKS = 10
ASSUMED_ADVERSE_FILL_PIPS = 2.0
FROZEN_FIRST = datetime(2004, 5, 31, 20, tzinfo=timezone.utc)
FROZEN_LAST = datetime(2026, 9, 21, 11, tzinfo=timezone.utc)
FROZEN_CANDLE_COUNT = 137837
REQUEST_FROM = datetime(2002, 5, 6, 20, tzinfo=timezone.utc)
EXPECTED = {
    "3.5": {"count": 106, "r": 53.647915196669246,
            "fingerprint": "64fcb569c981e1e7bf565a19fcf48248005cbe853619413cd9f867996c71735f"},
    "4.0": {"count": 105, "r": 57.90421353631645,
            "fingerprint": "60918dffa0a02143416e42d7cbd9f1eee90029d063bebb822a4f7189aec90bee"},
}
# Generated only from the frozen CORE25 rows of the previous uploaded result.
# Each row contains independently checkable timestamps, prices, exits and R.
REFERENCE_B85 = (
    'c-pOfU9V-gapix}vs++&v5J1n!(bj0%=FU)f#GqKi~x^pND7ca;Qu~F?tT02v&lLnx7?Ci#^tqc^*Tk?_p1N;j=q=ge*e$^_1&NT^2@J('
    '`0I~<{pGvge}~_v?|%2)zy8P1fBf~wzx?!v|NY0G>sNE}FaGeKKmPiMAAkDSAO5p`p9p?@?dKo<_{(2@@-Ke<hyU^4|J&D(FaP@E|NWtU'
    'XG-$L|AzcE<=>~|fB!H4mjCnPFTeif=O6#{$3Oq!-+uh_pZy!*`z(~i&wl>l-+uV{hoAoR!ykV5>DQnC-+6|1oc;12fA*j0U!?EJ|Ks0&'
    'L9WNgLH*8Oe*E{J{J;K9VmA8x>RmYa3j8UYi}u&!r?NGPe*NRm|Ng_T$Cq-+T)zFv-~H`(D-a^l9Yf}%I~dv*w691L43lbzVN&Ta%#sx{'
    'gBD0)Z!z?5;s8VY0`e7TgkgdXLrPdKzZNlM?I0{tItc&g*MIz{A%LX<I8#EgA`yUoMmGSzKoj*1W(6?7%;oV*p##jsgzlIn@-M9cMY&Tp'
    '5wi_t1Nf5g<xUK-Dj7no{G;iW%~U=wEWW?7f+%C;AbkP)nqY{W!5ujyl;vxB2hOj1UIs3IN$>@13S3T^!%Ld6gAMQ#wnT4H%Y}#U+##BN'
    '1sUJbd<PBb^0><5D+<U|HUM<ZL;jj<0-*m2pShg5(>%-sqE=Wbn}4QnF$AQ7V-Tx&_%)Rgi1{#4R}{6R%O{76D0b!+{FS!Fhfe+Gx=nkL'
    'aR{MijS$ql`^>q7Tt<+66DN%z)Al%C5i4><j297u?%vi<8`dH7>yckxlp$o03q_bgR@wzkG+O{#uAl*qK!Zeg)Tls~Z9UT-HDz446IyLp'
    'dVN!dxUpe)TTd73^}*BYt<m=&Dv1Ojx^x_Qc5u}*xZhipi<4)oS|}Z!McPZhqJ>sp=v2N!nqAzKO8E8_E4x})WPEiGf!vynUpvz7?gHDz'
    'zOUL`?4k?*Y;YYlZoUe>N6kM`i)^l+o%VPgHJ6c6fSL;w1Qh<>k-nm)is^#dYCr*;JJ{UBrRB@(a_Sd?`J;yhFV1?PdF|b}d3*j`vOkNI'
    '-R8bWlj>VPMpM6F;Ou2g$TuAnfn^AxLy$c(SynuEL;a1$34Ut+=Z_T6=m=YP6pyjM7^*4|jXgZ|kKp#m87I0Z9zS(X-0RtJkxMgeF6G3-'
    'Q!4R&rE<To^ga8HDB#{NpccdTUb4$9Is)-SyrGAuSg_o0uh_-%%42`UFF;w1WlN2>>(!lD3QZ&!Vd(KBZmOQk*zWNNwU(V2Cjg53lQ+~b'
    '`-iljL`mI^B_Oh8*q|BM8EhE}#BQFIT{n*aC{Q-Pu{9?#^o>)odr&4R>!)9$tIF1aMkz1qT76Qm<#uLy6GDvctDmJ^`&wlj`Np3Djh;C{'
    '<68_S%z61<_L)w<C7*8H3RZ5cV3MR0Ac*dz0G920NryKOU}Y~h>JzIZH&;Z+P5pkj%G`m*m}ng|{(@<mm}AKT0cd_kunQW4lmIlY>FS{a'
    '$6W4@8Ee7i)LgG$EUPX@^EYaoI*Sl$p4K9$K<}Q~0#sZmfJ*Lx^Cwi`;c^&aG39zy7g6h<Fah9o*s{O0U(@7TG8;!J5kEWP&>h(PlZ{}@'
    'EixU2_e^~E)VTP~T(yz?WmFsZXAx9xA_1o+(gSq1c~IcrhP&aYi_17ht;z;tYv~~d-9U3&c?sfaHFw@+Fv)NHvVYqW=QTw*i$~ovOOcb|'
    '%5C(UGMFwi4j;Eey!28>`UH^=zxje9WoEkOo)=N<Dm95^F&$TBq%Ww`S{ErOgqjDkYY|*Vwg9w4pt0te-t4HG(beP!L66jt6kJv}ZWnf@'
    'Zn<#ea`+i`H3#>B)skWRH|Gg(I&^SP>C!lZL`Mq$ikq79zOwaC9@mV@y0&riCuaz{Xz@Eo+i5qK5pxV$U>(TgHoI$E27Qn~>NU?@__CvU'
    ')}kqx;g&VjJk8Q+p2Ph4mk0FkQFQk_y0g}jmpoQIyCp9f5^LH{UP9^1`ed}Mdwt4gh@t!CkwfQay6e!njG4!%wU{Az>GbF=xawXJGu}>='
    'Q<w5(MdW4O!}=g6nd0X99L1#Ag;{Q5Q9SC__FG1H523=aMtI7G<6H=0zE`}E)^U^GuBw(*yNWF{J+BO+YgF{o`I&6LbgE<lRNRtnqdSUv'
    'cnnu<OWAGJwzl;J1`lVbtfN+InA7;wKgYC?C&vetq7Z0+U;ct+Y$=dPEDXO;RA9RUr#Bg;BwGj4Rw^}+EdtMvbQ`>qBJd7zMzJeo5e&ie'
    '>tV~DaOa#zIvk1Rr*0|Ni>ZXctDoJ2S9kER<ZpjZI&l)Igcw!@9A4n0iDLx>O}d;e*RI$lgu0plpq2*XY2E~jg?TwFx}BF3N@e))(MMCn'
    '&_<N5zE8-n>bO|n??7V~T?dU<T5&L&MS-lJ<=t#~l`#dN9RiImEM_jZyqudVzGTanb4}KvmLaW2WYp@a3)v<NgIC5}x?*Z!6W32l#&C2~'
    '#1Ph=%K$<RA!IPQRa4pj=sE3&7M)WprmGcw;MR7q(0#}xVa0)ob}J4pBheT&f9U)PzLr2fv*_-B-vTlRjV_MBvf}lYqA<%kYFHBi0JGYI'
    'sD-r$k=;;23dyJ(eF#*O)ikU<Am%%2c<nP-cJ8cex^eA?ybc?+8!_>T1-XPJ8)V%!4emiiuu%+~zi4-5lOnL+FlW%CMzo=9d{1Bx-#F70'
    'p~iN^;5(@0gu(%8t?6cHP-}<TEv+fZjn;ne1MmR0k~Uzgt@86ympj#w3+!jPY{RBGhOOGFGK3B7W)B4H+GnWCceFzdbva4G&B@GjWdjW>'
    '(0;aEfp!^(jfJN`^5Pj?tSH~h_Kci$KdlK}*qYs+S=TjarQhp(!T4cIn8Syy+Wbw_IIyCJKswF^3n1PfHh)xYzefc65`M0=s%8I8n%$Zg'
    '@!5Y;__jgTKCfA4FpvZb1K9iQQm!o%BNU}NN43RZm>e!gI`yi1ri_o9A9BsN>>x*Nq=jeMOqG@5q5EUK8_XV6YRT+I{%jnMLClmQ5UDj4'
    'j%IIau9dq|B43BT9yfXUGhlfKPpR$IvkW^Yqp)tB47$*t+-WVfjlBL2lh#a^St&sA=tdY;vE9MDSFt^?#eE0QIu!M6Z&*VypRx8doTzp3'
    'uz~3)0pB9BB0{3T?t|L<enW+$*wuk}NQYy`MbOIKNw(|I<Er1%xyVZXu`XJYI*CxLpR;7UNa8RalZ3nWS1HEq5NK4S5YByMQO*Ch{q3?R'
    '>H%k22Tj}Eon%kkpAU2Gezu)!uQFl)n%*Ci&)cd;1KEp*?9uh#p*vV5XIlrYd_kDDR9A6BPfuZA(<F~u!9D~UKXF;K+#7XL?ZMymk4u-`'
    ')QqglsMRjgJk1a@O3GoziEY=@(+uOt{}3pR8+91$J^{h3qWeA6{iCJ;>yH|=F48$@AGx>@Xvo|5Q;%ur5okw_x{QO0d#?n$KSKMxn%JqD'
    '`OjVrD%xBc%yYsubPQF^Fz>3G%SgiudI&@>5%o3hPjL~43zWzCO!FWd)<M$-+AQi<4I?o>OZ%W5LC6R+t)Ri!6r_9j>cN-pj{<kmtGVWv'
    'b<FHOyEo31a0pr9T{}=E)BrJy8#c{SmnU)8rR+EA_aqVS?%6tM^$A&MKd-a-^Z3iZZ5u$het8UFD^`z9!ZCh$g|EnEI%FNbK2)ePPOFbw'
    '8+WY5%$c7{6c)VvY`frPWMh}jPBwutXmy09O9j*8O1`;!fa^$Mn^c|6glG!qe*G-%4w+XOAwcR7XgCv+1#ejqv0P+-kfv8JVbFEZ_zRS2'
    'c0>w_hNK!uyA2RkMh-wb1RC>b<ok3l)b!HoNJx*AEtl)<gD+@y5$&T~HKJ+}vTdYHQ-IVV&={$sH}B>BQndYmv#-fm7Fp&9s$%Avm!4kB'
    'oWrV&Ul!~8;;hnvplYd&ar}NB-~1bLfMRZ8!I1GL!+q+(ZGeBk+aB)G4fzVABHOz1Sxf)U>ZqDuj8yo{<$XQW6zZY2!oyGxk5b!<8d^&G'
    'b?E+;)504@v@{<CXET;oay(<H+s>{Qj*Ms%XiV7x(hZOkBrU(aGtNwDUFSStrZjcj{Mv@&_i1}PsY<A0$WC!a-mWXvJylfO&sXz9)Q3`5'
    'mV5Q>98a1xa`_7vlH2$h??Y7}a6BNU$dF8T?Gmj$)Z1A#P916`0dAo-&vp149FOp3*e;+tYKkFh^-hiosBZDP5niSB5nhn~F8V{bOjiiy'
    '>_5Wk6NcePji2FYu)&Ri2qT;`LvOGvIdXM9lBy}mF^?B^my`4Q>|l2}OY#qXQO(N7U^Y$NHhSZ>!LGS<8EL3y<!t*LtC``Q^Q?xQ@+76!'
    'S<C{m+}=={8?Ms{F0n+R*JcTJdua*;CFXK6>WoWL3QYIs-#-A}F4Xrky&m|m?p)K{Cf{bPq9;Sd_?i;fpu}9vBk6*L=-};qgCg=c<55md'
    'v0;*}vUGM7UWgB7?~C*(=AA2>TL?H~#2|!_u#jLoPvoC;j!}b<?nkkE-Hevo8#<&;>ajJXYE1*D?KBxVq{PffWjEzfSW6irbqW*^BU$Iy'
    'I#m0UTG-!GkIB~GHfYuDDWw@o*vji?+pWAs*aDLPM~-huRO3PJUBAdZw$*x6e-))*+JUNexy@)B-317F8xv&RhRViO_Cp{FqX4~g<^f*Y'
    'JC<7$x;jD0iI=@KOwC)PY0bDw90uv7E40)1aG6~>E$<r;OGzP!wRL=$^QfaYcb4CrX}?E?`nS!Utg-{5G<NA37hFgum1ST^{dNBQ`+dWk'
    '%bP}BMyF^R#P58(4KLJ<%ny<t+$D|t|GPf*QN=taD|AjFn%_`}_FYDmi?Wg}ddvWlC*DZmoB|Z}ZoaOBqW2L7S3+G*eRR<Ie-BsNi?Gpp'
    ')Y{sN@|TK*ejJs&8#YuShjo{45{+rO@c2=$Dwpjs!8FtwHLZiynrP>01{%kvAh-{f$It;-*mC1G^m8-x-0#;Gna3S$4qL%RKNlP8NhSH8'
    'g{FSl;=GY2-G&Vdj2zu>5{+Zi%y*IpQGY}ChuNTSU8rrJvi`6&Hj2veOVeqr7&@lZH=$z94|#0HiIT_!N77*f2sUaK3fNYK?B4YuHT2r|'
    'QU<rS5sfaZ-_dX}EojB_r(*l_r%gx-VAFe={*oF=MLXvX`2oRn51T@fW!Ng`Q<-<}=3_5Kkhc`xhE2h24BH{mxP&Fvr+X>czP14ma6`@;'
    'm$n9nuWsk~(pr3H`+X~5$plg+!(v%yAmT7B_f5EQeu<Ao6)$ZK`eb|tN6JTLJIc4x4Z@b+V95I>Ln%Xyn+124%6;@h)P1;R|99}hxjMs+'
    '%Hh4b8}x)&+!E5ezlcCwM?nNsjVIWhwfGTs%}W_$mqqUv*EGX}RPX@AoEG~Y5L>@bnpd|}V>6-oftj$QA)tfpL!z;lqSQ5wTS-rvQU?))'
    '`#9^$*En}>L?~kp(Gos*ewGK5GUiw-)K(L*$&g1CdPw+q%4fmEV7Kds-3?f7wSg}72dL!qJK!}Ulv!gEy9XSDm!$&_EB}W<CamLZ6LSHW'
    'Oatt!@#znHFQwbL!ns_h%k*i-5&&OXNv2oVL&jP%pS9jFgp<9(uszw^gyb=7r$l3GaqN@1&5=LgmhMZ2E)wF7_MItWn(|ABoVJyFy3anL'
    'e|#VR%xL5)jwQko+hqCh2}HeD!-NaUI<HNSo}{$?@KJl9=LH-&yF15vhcs9@RrhnbM~hl_1WeI8$of0<=%a;o=rEJ^YzBtu!DhG~M)ws~'
    'vjdG4B`qwS;uQ8r_}O-UL=|$_f%7TRaCHqO?lMs8yWvvm!7^Lz%&(#~KB{TD&M;m?XeiqYroc^b3BYRG2Zu8t`E>cO%Lf3h>{r72N^326'
    'FfZd)D^D<QjLQ}$+3WP@E^c-7KEUm03mVtLGVwjLZp&R>+&jVuw$7rAjXVbDO%eH|V;^71BDrT#b_@%-RUo50TLTA2MBhQjC(-{n>B(}h'
    '>g3wCy$Ibv&HNEwO%NY^HgmeTd=40n)eN_r39FE30anyZ)KJY%pYlb`;L1$+8%!yZ6kf+|Z3P@jYy(L4Z2-0k^BA|b18%Hibf2!Tr7(J4'
    'gs8NXxN|*ot#-|tq;rbNru<Agr_F<XQqhvDX`Hm)RGD)Lp^L_LJ=D$jmksJb$hPk*edJa5BIk9LYa3vvUB8v9b7cXSNZ!^y;bE)!DICAd'
    'U|b4@Cep2@NoAiRqwm#puQOrV(8QV)lPTL?6`2-51fy=V6s4S~Er4(}IjC6QLRuoNtfEp-Rs<?3-bYAex()6iHmlzJdTb^ioN_a8*$vbJ'
    'xQx<)i<Ld&;LuQzGQ08`SY&xuPs>#LJ#qXbGv9BA07|mH$Ggg@E8U)|C8US+v-@rXs{EuXX#R&X)1%Yn&9c@MT>C@3z$dIf;J(PKFg++s'
    '_H(ZO?vHkPM=*Ynx=h!hTTI*MXFW+1VG;iLOK}3D>(#Kjdx#gU9@TO;Ky6O)DVa6Y)cC4@n9pwGX&AzkL*STrI}C9lhYjtgL^)d1nz}i}'
    'n(f)=B+UbrQ>3(xo3(o@(@t58yHqf3cd69I9}RG`_BivnOQrNJJ?swRzx?hy_MiFte_pEJ_RAh6FT06J&bi51R@6Vz6y^%f4DZV=y$n~n'
    't<-N}9Dkoi3p0(zd*s`q`$+MQ-r(1zz=<C$E$p`}Gq3l#H^#`8$en3k(m3IgRC*BIZdI}*t3epSTMUCN^C4WALrBnJNc3c}a=%ud%WB4|'
    '%2-<QWtN+_BH^^U6?&7g8e>)f1I(J=KhB9-R}Jz&X+67_dlF$;CqHZ#qGjV}Rqv9+HuuUd(JR|^vIdTUj1W0NlreI({eq%7Z0mZ>YUOUV'
    'Ri)=(O0W)GZsZYZp?y1t>4TIzcSH;1Z@-H0Ti9~p;X~JC1C2(;?qg6H`NH|VOk%}@1Ps#-f;B<9kcH0&E!;SBmAO;Bx-X5}N^uX~%-$Od'
    'KpL)fImN@E4>@;b$ZXfG-s|%`y_b?@b-}HEtQD9kp@uChezg0}xrF2W@!!N52D}R0`gpyx`K)s?_l}*Y!8&AXc|XX7-t=wLE@+~|^|q%#'
    '66CI+TMgks^>S|6`Z}5JTv2Va^l6@4W(#>=J{+r8>yY8|7ruYxJk}Y~G9S`~K^OZ25a(AkKXL7#&(#;niz(;U)zC<5W@!4Y2vaJ#yjv$U'
    '2C&HZz8(Sv!|Zhgp}Vjw4_2o25o~*$*HPn@EkcSC?~g~xyop+EsYwB9je35lcK*oS*$!1rai2~Nq>sf2@^UgSHS&;Kr}Q}8Y*1Fx&DQ&P'
    '?9r-8!BT5)`W{a@3g#m`wWUde!Nxw5SJH!#seb*Tdn;*O3)}AF>A+O2Vnp>xKZCrhg^UHpsP&r!KeaIDJGqXrN!~ZMJ$>W{_wkw%mXFQH'
    'Gu)`ex0g!p(#XYM5Fm7)o$Jcd_aK@KY5q>>0<IXva-Y_7H{-hD>bb{4m8WM>^~&NxFijLT68PD>MuH(~E~DiDHEVsz!yG`#_kHOOq{x1O'
    '7gN6k{2c9HgAJOso%$}WJ+Y%_W!KRo01A}tXy+b#$En=6NVJV~4}6pkG)j4uQ8Il{dgN(yUd$Xrn0AeAmC=ID>4!ivrc3XbV>t8U?FW8G'
    'Kz5MA1@29txOW?{Y~M;cn7ad{UPpQ_>zCxt9m%kqjO#bdGcHpCq_)K^&Dn4z3&h>V1iL^nj5)s3yZQs4{Y5X$*(mUL2<(Yv)!k^mMk5nR'
    '(-k;jlWbj&W80u{p#U_whs_tZY7drU-sNV#pK`>I0Ps3$xrM8<O)lTYbOxx^vi~k>{=r75<(Nj8xMM&}xV%WdUxHruS>$D?4q~;LbtJ93'
    'bmpsDRB$&eb#ccGeur!@Hk6bz-4}TSUSfHCOtD6O^|#icW6cjvS9#0vRt{kkF#1jDOP3i-unxVAXW__6bIrpWid1_R_lNHKQdeSGWg}{{'
    'yr|Qnz6Y6MZ-k%8u4!-?IoctA2sG9_ySFZqA?N+>b?VnDC(X8_dfYDWBqc(*goJf|))UrcQK7oMtp3}00%E`>c}SPWS*3Hu%L|4H(t~MF'
    '?lH7wUdcm7nI@G;ImK@tZKvW~#$f}=DNtZ3$ip|gqo&tw5czQugewi%j^<els$hm&4(TZTOghc8^7&UehS}br=ze;1f2}1nd8~SNUzP?!'
    '57sRA91F~}%>M_2%*}Xf+A-TYUX=#A>)W}E<E4;?Kp4DwdUP3Fb#I+2rk^rq#Q{P8n`P*2IqK}Ca~+PtQS4$Yf9HyaUU2@#fbVfr7*+^R'
    ';c%P>LCp7hS7nM@^-g2}oGx!5!Iq+)*9FnlD0<%fOtzmlJvfj@P;pDN4fn?D;WAuREcq+&xV+#o43%}Lv<<^f!c(M@7P92{z$TQV#dri-'
    'z7IKnf<`13#$1>a=x^t=cDFj&I*_(fr-2*}#8#HvZSYEpz&pel1+36=YY}*0u)IRE&y7gp^)`}2%vz+G#9^397_#~qcC*-GR&QgjQv#>0'
    'h}&-xse~Y68QW7#dQFDkHPBJC2ej&aTTUG%U(Eo}$pAcsv&6(ZSadrpC;njBc$NIw5;3$9rK|1}zU+H^%)2AZqU)gXN+S+t3-{pEuEO0+'
    'd6h8*pdA8@?k;97w+x+QMwiEVHH0V~3q>V8Uk}6D&@L^cn=p)C8FR-&=^%sqT(t>+s<-qW!`gEhV5lL63?}!Lw{_&@K|s;D!eY9-SBOvM'
    '^(o;KQ%D!<XS7>xa2dxttLcNjGmqKD?zD;0coX+EueTb7H#U8rQ#vySREUT1{7iNO2q`qCsBZ$LJa&X7VZWcXrnOe7V!ECiSNT;YHaNy('
    'dFlxZ#&aWnrrQRg7)4l7j1_Q4kdT@GE~|z7e$$4A@jY)nUi+xwVj;F{yQt-K42+FXYdtqZGg=$W?xY-0=_~hpZg>D&NgJ@${`h&R%f0Gw'
    '$7o5pY{RBGhOJtt;f_(mJ7>6{`;z-cpr>=nsLS~fZcb*ND;sE7bFTZIca4C{IP5Dt1queS8TekPXB2(VS_56&nproD0N|ZyW&E(EaLH(m'
    'fp!~`T*k5K<ddm(ycq|MDXACc7r2I~tGcE=L4D3?mHMUbu<xGVv9jgR9{Aa|J*Y!$LqAR096><LSr_o#Q<g=SlSq(y+nG77r{CnPhs@eR'
    '$bG(*Dxn>uuCMNHEjxLzTBH^o%KLIx&Ig^%2u5m6grgZ7I_aRUFs1?zNW%uor)j`B@di+7OW7yx2q&YkLY)k{aNobZ7VwVWXJaFplmZZs'
    'u7hE@+Wok$vX9q&bN09^T|s$0XYw%%Jm1@@;Zka;NcEAIQTdoo*CcOIRLD74?42-Pr*mxgePQjyCsPppGIYwiz6|}~*q<UOiNm-{KFG1w'
    'UqPi9vqPZqy5QVL5E&kr+%8R`9$1!j(6rs%*)DXA7f<-vc2d2{hyiGaK;yOE(2;c}1=t6zi}2Oaw{_4;WoDW6O;>S4=T2c?(<G0(zdi&S'
    'zj1ZN=iT1YHOsk79@bA6o&-#5sMS`{JnhbL&ohPV(2;G|xzjwJ**^tJ<CYu-yU#qZB*VYQqgFL`9W-3oddT>ZbQ^(&ynR3Q@Uups9m(i2'
    'jw9}!5G-_^Af(4F`sP1-HI8WGXE0Cx)@U(QHN(8CYA)lD;Qthe#_WDI6%rS2{H`KCPF$J?-ms3EHq>U3zG@hK`B~aWEsr;B>Ic97*cPPD'
    '6kK=Dtq1xYX0G~W9W%R+>y5J`970xj*AP?*H9*Yb-b}M}&y7GadeDyFQ{Nb&t%FwIkcBq$I)uNlom$E^fCS@(hfTDU$2Q@ZID8>BcqyMR'
    'Pi<KbA1B(_P7O<>{zPGE%g?q;TSg|)cIty4cL*A0)ULpgbiW?hZ^S+MfEPi-Hm91W%>LojZ!KT#&XrdggB7@^Kr$xtN9<VXDgQn8pbLYp'
    'gT{^Za#qSJDB=2ug0$NJQDx)+v_qgVcSgsu1iG9i=N>3qF45b`5tOF8aXu<k!>z$&+d$cn0_{_vF;H;lRg&7IxbLR*qYaIRXF2O(ts7|G'
    '3g^LDSd)=T@ZI&B*T5uEGBwL5hjWZ#X3YVL`MwV1az~*Xbuh5JkK%=)|C>mPZ0pKrt@m<PI@KBp&dsUjeLd6^>Y=u>!%z>8LfeZ$g{wt<'
    '+^}z2c*C5Q`xdEyeDlrHL(^@yR|`kxv<WmOX#weG$7_Gh123cl&6L)4&SU4w=QX1)Oc>7Ir|p@fDxn7Z<WsySqFA?29!g$ycBv1ltSmR>'
    '+c}doU5Ar%yn!ia-iN9{;CDdGkKvwKzx}IRNnPpjB$AZ`xP{t0?cr}QzFC^KT|TYL{4hif0<Lrs?M8NWnPwh(`rvAdmkA2%y7AU<gwqEM'
    'IbNrUIGA53wdJFb|M@7rZ@2WfG4gZkN-c<ZyRf^O0+=6j1D7N_d*ks=`xiKE^u}p}U1RAoQn1(iX!IN_nc<z|Ok7#>1L~TdMYMp}`a1N3'
    'e0A!x*|~6f2wQ^PYIA|0+FVXDopEhS;lA7^1KWe`nb7w(c-XTs;Z{D9fK!%l3iZv;`W4lNZOC70@~u`&-@}MJPJNV<L~NLCtGt}pZIw?C'
    '=8XC!-zS&W;(qyb-vB+P|KX6?F(Z}TD88_k60Al)1PXY5tn<~}x!(8H`VxH+N$WTE+)kCI9Q|QOs-JClq!wWd9y&NOSVOWEM7rAq(q)?<'
    '(nD1+?Lbxgwx$f25bofsWR$uMm5qzYhd>m1wt6Rl1HYF05mY~#RdV8WlKs?t5SjWQRpK!4EL|O$zQ2-qCX&h9CZ(hhMS5K&dqFW{m)KQ('
    'a83I;5C0?S>QbtMSYd(NEMBejz!|aQ9T}`dKe`vjQ79j;!3+DK<_Dz&=~5qcXQu5aUK<Gjol{@tH`JGX-;AKtX>iU2CwX@aV&NR@m9Ns_'
    'IDzEx$-VVHH$Qu|iqP*H1zc^H!A3|?OGPuLS}GQE!I!+Na8<}*8R45mV=gH?e$<P`)h|6>7F3-VS_iB3flmDn0F6T|5ZnjLL%{&7whMT?'
    'DkNp-Im@pvsXT37aM%j2?j(eBpUE`B%8GIFLz8a9h6Qr4GCtn0B7Xhud2zP`_#jru{eHq!9lwoD)HSFnbG~<`7&?DV`liZM>Y_xdvob{{'
    'Fp>`Y8L)ARP(T+dmHnct_&phB8(dnE)iIeW<k3DbEy~1mlVbaGlTAnpVAFf5`4Zr#qTNa_eLO7*NukJh8;jzh%!k(Uu|pw9xCw7#rC=5='
    'im#=<q01)L=PCKRzKI`4RxT25te1k12ynW**5Z5GF9)d}R!K=yK}?i*h9VAAN~d(ejFGTdzq<BG9S}#_ckC=(*1naF2DbbLMBcX(N*Q9@'
    'ELuEWq^r&|{)Xk4$<=vF<r)pvbpB71#jOmz$D6PcPca;MotbSoi^wDBnrAYGE{onTuB$meP`Lq!IjwHW#MZKs=4Bbx*fwZ>U>oeHWREt)'
    'H;KlciBi}2T+($w3m=cQ)rzaEFY9guF*gE_G4*8$A3Q(Hy<_6u3b4moivB-gvf;vo9?}z@@>wwG*6pS7S7UiE4fLxr0U_<Ii9h%|X8kmL'
    'E|d*uL#4w`nXPD%0!H~HmX5IlOr`;L)-n*o_Ce{^gR}<s{eh#wqI16VKuoW$hl#b;JN1kq97vTq3b%W8A=n@FDbd(i9IH=WykP{~&n^wt'
    'HUcB<T-_rNJeyk(!jOPfzO9CPu^cr_J^wkfx9@Q*5sugfqK8i)>b;OtV*z{6M>xPyQd-B6+EZN?Ts{m_b$&*D*OC>FT>Xu*x`{STaXjed'
    'N4w<VK1O@dNA1V)6YD;$&EOiVtq!eX<MK!go02%iyQ(>Dm-wrY!zPkXiH56J+z0RSPln5PH}wetFk9_2ud*GKN;9+!<NbMtvb`3mE=eu{'
    'T5W0GaHJuh&i|D#9~2)vdS$Rl-W7tEajTUkm^XK2i?iz4w)YG`9Uc#GJ6c=DwXaNk&#RkXs?{7oe<jh4;CeQ_k)G7BXSy4yXqZjsBDr@^'
    'QX$NBeya}Z;siVh79{gYP(RLkvfRr!d7blYpl6878)*+pmia!5d=AWw^$fQ=;;N8n0aVnC)KJe(2lGYGDsR(vif&*`k)-fCZfl#%NMb8M'
    'vTp@kNYwzhwz@3X!F?YW&z$wa2uL2N8x`7u4d?ylImK*Aezwh)<U(4s7rsgqgbp6^-j_vZP<qg`Dt!p%Mh){iZf$Gnv~3kdWo3PoNZy7}'
    'g0<y`aQyP2UW$b#((RDG%Ba6V^(s%=hLNbbF<B_Nn$QA>9<y@q3AddEb)+MmbWHCMEs;`INhzo(!nLopTc$@jJs?5vy*$)|mTnDO`Jh3Y'
    '^#CyH3bu^WV_TUZ<oum*wn|h3TP*KNX_-pD;!Lh0FB=%>of=AFTbSxf)0k=rISu{n&dBdTs-PwJuS^|ipvi*K6x{nJU*r+ipK)J3Rr0mP'
    'XVp2R0ghC1TE;P-jCfNY$70$!KhwU?7*hWBmrmGns?bBa`BT1Vb*7fP3DP9%E93ffGYKrahxzQ8orWPyIfRahx5E$@a@dxAN|d8Lqp2IE'
    'tAr){!_|GOF_0pqb=<7=UCjKPZ&|64dx&Y<&mlc}_(r(d{eiwVsg$mxr{)d*_Ah_?|3MD669'
)
REFERENCE_SHA256 = "57a38d607ad1bf0320850bdde4173fea0571392ad28c023a862c8b61c52581fc"

OANDA_API_URL = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com").rstrip("/")
OANDA_TOKEN = os.getenv("OANDA_TOKEN", "")
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID", "")
PORT = int(os.getenv("PORT", "8080"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "."))
PREFIX = "euraud_h1_long_independent_confirmation"
BUNDLE = OUTPUT_DIR / (PREFIX + "_RESULTS.zip")

app = Flask(__name__)
STATE_LOCK = threading.Lock()
STATUS = {
    "state": "not_started", "message": "Waiting to start",
    "pair": PAIR, "timeframe": GRANULARITY,
    "orders_supported": False, "trading_enabled": False,
    "frozen_cutoff": FROZEN_LAST.isoformat(),
    "parity_passed": False, "live_orders_sent": 0,
}


def update(**values: Any) -> None:
    with STATE_LOCK:
        STATUS.update(values)


def utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def write_csv(name: str, rows: list[dict]) -> Path:
    p = OUTPUT_DIR / f"{PREFIX}_{name}.csv"
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with p.open("w", encoding="utf-8", newline="") as f:
        if keys:
            w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            w.writeheader(); w.writerows(rows)
    return p


def pack() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(BUNDLE, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in sorted(OUTPUT_DIR.glob(f"{PREFIX}_*.csv")):
            z.write(p, p.name)
        p = OUTPUT_DIR / f"{PREFIX}_summary.json"
        if p.exists(): z.write(p, p.name)


def refs() -> dict:
    raw = zlib.decompress(base64.b85decode(REFERENCE_B85))
    if hashlib.sha256(raw).hexdigest() != REFERENCE_SHA256:
        raise RuntimeError("Embedded frozen trade reference SHA256 mismatch")
    result = json.loads(raw)
    for rr in RR_VALUES:
        label = str(rr)
        if len(result[label]) != EXPECTED[label]["count"]:
            raise RuntimeError("Embedded frozen trade reference count mismatch")
    return result


def headers() -> dict:
    if not OANDA_TOKEN:
        raise RuntimeError("OANDA_TOKEN missing. Use a separate research service.")
    return {"Authorization": "Bearer " + OANDA_TOKEN.strip()}


def fetch_candles() -> list[dict]:
    now = datetime.now(timezone.utc)
    if now < FROZEN_LAST + timedelta(hours=2):
        raise RuntimeError("Current time predates required frozen reference cutoff")
    end = now.replace(second=0, microsecond=0)
    cursor = REQUEST_FROM
    by_time = {}
    session = requests.Session()
    chunk = 0
    while cursor < end:
        next_cursor = min(cursor + timedelta(days=180), end)
        chunk += 1
        update(state="fetching", message=f"H1 chunk {chunk}: {utc(cursor)} → {utc(next_cursor)}",
               fetched_unique_candles=len(by_time))
        params = {"price": "M", "granularity": "H1", "smooth": "false",
                  "from": utc(cursor), "to": utc(next_cursor), "includeFirst": "true"}
        for attempt in range(3):
            try:
                res = session.get(f"{OANDA_API_URL}/v3/instruments/{PAIR}/candles",
                                  headers=headers(), params=params, timeout=60)
                # Earlier than inception: do NOT suppress errors once history exists.
                if res.status_code in (400, 404) and not by_time:
                    candles=[]; break
                res.raise_for_status()
                candles=res.json().get("candles", [])
                break
            except Exception:
                if attempt == 2: raise
                time.sleep(1+attempt)
        for x in candles:
            if not x.get("complete", False) or not x.get("mid"):
                continue
            c=x["mid"]
            ts=parse_dt(x["time"])
            by_time[ts] = {"time":ts, "open":float(c["o"]), "high":float(c["h"]),
                           "low":float(c["l"]), "close":float(c["c"])}
        cursor=next_cursor
        time.sleep(.02)
    result=sorted(by_time.values(), key=lambda x:x["time"])
    if not result: raise RuntimeError("OANDA returned no complete EUR/AUD candles")
    return result


def raw_signals(candles: list[dict]) -> tuple[list[int],list[dict]]:
    """Scalar reimplementation; no imports from old discovery/refinement code.

    The current H1 bar is COMPLETE before evaluation; ATR includes this bar.
    Structure uses preceding 25 completed H1 bars ONLY; previous H1 high
    excludes the current bar. No H4/D1 regime, session or weekday filters.
    """
    signals=[]
    diagnostics=[]
    previous_lows=deque()              # exactly 25 previous completed lows
    atr=None
    true_ranges=[]
    prev_close=None
    prev_high=None
    for i,c in enumerate(candles):
        op,hi,lo,cl=(c[k] for k in ("open","high","low","close"))
        tr = hi-lo if prev_close is None else max(hi-lo,abs(hi-prev_close),abs(lo-prev_close))
        if i < ATR_PERIOD:
            true_ranges.append(tr)
            if i==ATR_PERIOD-1:
                atr=sum(true_ranges)/ATR_PERIOD
        else:
            atr=(atr*(ATR_PERIOD-1)+tr)/ATR_PERIOD
        body=cl-op
        prior_25_low=min(previous_lows) if len(previous_lows)==LOOKBACK else None
        wick=min(op,cl)-lo
        ratio=wick/body if body>0 else None
        if (prior_25_low is not None and atr is not None and atr>0
            and body>0 and body/atr>=BODY_ATR_MIN
            and lo<prior_25_low and cl>prev_high
            and ratio is not None and ratio>=LOWER_WICK_BODY_MIN):
            signals.append(i)
            diagnostics.append({
                "signal_index":i, "signal_start_utc":utc(c["time"]),
                "signal_close_utc":utc(c["time"]+timedelta(hours=1)),
                "reference_close":cl, "signal_low":lo, "previous_25_low":prior_25_low,
                "previous_h1_high":prev_high, "atr14":atr,
                "body_atr":body/atr, "lower_wick_body":ratio,
            })
        previous_lows.append(lo)
        if len(previous_lows)>LOOKBACK: previous_lows.popleft()
        prev_close=cl; prev_high=hi
    return signals,diagnostics


def replay(candles:list[dict],signal_indices:list[int],rr:float) -> list[dict]:
    """Independent trade replay; stop/target assessed from next H1 candle.
    Intrabar both-hit convention matches frozen backtest, NOT real tick paths.
    One open trade per strategy; signal on exit candle eligible.
    """
    rows=[]
    pointer=0
    while pointer<len(signal_indices):
        i=signal_indices[pointer]
        entry=candles[i]["close"]
        stop=candles[i]["low"]-STOP_BUFFER_TICKS*TICK
        base_risk=entry-stop
        fill=entry+ASSUMED_ADVERSE_FILL_PIPS*PIP
        actual_risk=fill-stop
        if base_risk<=0 or actual_risk<=0:
            pointer+=1; continue
        target=entry+rr*base_risk
        exit_i=None;reason=None
        for j in range(i+1,len(candles)):
            bar=candles[j]
            hit_stop=bar["low"]<=stop
            hit_target=bar["high"]>=target
            if not (hit_stop or hit_target): continue
            if hit_stop and hit_target:
                # Frozen engine's explicit ambiguous-candle convention.
                reason="TARGET" if abs(bar["high"]-bar["open"]) < abs(bar["open"]-bar["low"]) else "STOP"
            else:
                reason="STOP" if hit_stop else "TARGET"
            exit_i=j; break
        if exit_i is None: break  # unclosed trade never included in frozen closed ledger
        r=((target if reason=="TARGET" else stop)-fill)/actual_risk
        rows.append({
            "signal_index":i,"exit_index":exit_i,
            "signal_time":utc(candles[i]["time"]),"exit_time":utc(candles[exit_i]["time"]),
            "rr":rr,"cost_pips":ASSUMED_ADVERSE_FILL_PIPS,
            "reference_entry":entry,"historical_fill":fill,"stop":stop,"target":target,
            "exit_reason":reason,"result_r":r,"duration_bars":exit_i-i,
            "signal_close_utc":utc(candles[i]["time"]+timedelta(hours=1)),
        })
        pointer=bisect.bisect_left(signal_indices,exit_i,lo=pointer+1)
    return rows


def fingerprint(rows:list[dict]) -> str:
    lines=[f"{int(t['signal_index'])}|{int(t['exit_index'])}|{t['exit_reason']}|{float(t['result_r']):.8f}" for t in rows]
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def compare(rr_label:str,new:list[dict],archive:list[dict]) -> tuple[list[dict],list[dict]]:
    """Require identical absolute indices, timestamps, prices, outcomes and R."""
    checks=[]; mismatches=[]
    target=EXPECTED[rr_label]
    checks.append({"rr":rr_label,"test":"frozen_trade_count",
                  "actual":len(new),"expected":target["count"],"pass":len(new)==target["count"]})
    checks.append({"rr":rr_label,"test":"frozen_total_r",
                  "actual":sum(x["result_r"] for x in new),"expected":target["r"],
                  "pass":abs(sum(x["result_r"] for x in new)-target["r"])<=1e-7})
    fp=fingerprint(new)
    checks.append({"rr":rr_label,"test":"frozen_fingerprint",
                  "actual":fp,"expected":target["fingerprint"],"pass":fp==target["fingerprint"]})
    id_keys=("signal_index","exit_index","signal_time","exit_time","exit_reason","duration_bars")
    price_keys=("reference_entry","historical_fill","stop","target","result_r","rr","cost_pips")
    for n in range(max(len(new),len(archive))):
        if n>=len(new) or n>=len(archive):
            mismatches.append({"rr":rr_label,"row":n,"field":"row_exists",
                               "actual":n<len(new),"expected":n<len(archive)})
            continue
        left,right=new[n],archive[n]
        for k in id_keys:
            actual=str(left[k]); expected=str(right[k])
            if k in ("signal_index","exit_index","duration_bars"):
                actual=int(left[k]);expected=int(right[k])
            if actual!=expected:
                mismatches.append({"rr":rr_label,"row":n,"field":k,"actual":actual,"expected":expected})
        for k in price_keys:
            actual=float(left[k]);expected=float(right[k])
            if abs(actual-expected)>1e-8:
                mismatches.append({"rr":rr_label,"row":n,"field":k,"actual":actual,"expected":expected})
    checks.append({"rr":rr_label,"test":"all_archived_trade_fields_match",
                  "actual":len(mismatches),"expected":0,"pass":not mismatches})
    return checks,mismatches


def pricing_preview(last_signal:dict|None) -> dict:
    """Read only: GET account summary and bid/ask. No POST/PUT/PATCH/DELETE."""
    if not OANDA_ACCOUNT_ID:
        return {"status":"not_configured","note":"Set OANDA_ACCOUNT_ID to check live pricing; historical parity still runs."}
    s=requests.Session()
    summary=s.get(f"{OANDA_API_URL}/v3/accounts/{OANDA_ACCOUNT_ID}/summary",
                  headers=headers(),timeout=15)
    summary.raise_for_status()
    account=summary.json()["account"]
    res=s.get(f"{OANDA_API_URL}/v3/accounts/{OANDA_ACCOUNT_ID}/pricing",
              headers=headers(),params={"instruments":PAIR,"includeHomeConversions":"true"},timeout=15)
    res.raise_for_status()
    data=res.json()
    p=data["prices"][0]
    bid=float(p["bids"][0]["price"])
    ask=float(p["asks"][0]["price"])
    nav=float(account["NAV"])
    conversion=next((float(x["accountLoss"]) for x in data.get("homeConversions",[])
                     if x.get("currency")=="AUD"),None)
    result={"status":"success","read_only":True,"instrument":PAIR,
            "account_currency":account.get("currency"),"nav":nav,
            "hedging_enabled":account.get("hedgingEnabled"),
            "tradeable":p.get("tradeable"), "quote_time":p.get("time"),
            "bid":bid,"ask":ask,"spread_pips":(ask-bid)/PIP,
            "AUD_account_loss_conversion":conversion}
    if last_signal and conversion is not None:
        signal_close=parse_dt(last_signal["signal_close_utc"])
        age=(datetime.now(timezone.utc)-signal_close).total_seconds()
        reference=float(last_signal["reference_close"])
        stop=round(float(last_signal["signal_low"])-STOP_BUFFER_TICKS*TICK,5)
        adverse_ticks=max(0.,ask-reference)/TICK
        bound=round(reference+10*TICK,5)
        risk_worst=(bound-stop)*conversion
        can_preview=(age>=0 and age<=120 and bool(p.get("tradeable"))
                     and adverse_ticks<=10.0001 and stop<ask and risk_worst>0)
        result["latest_signal_preview"]={
            "signal_close_utc":last_signal["signal_close_utc"],"age_seconds":round(age),
            "reference_mid_close":reference,"stop":stop,
            "max_accepted_ask":bound,"adverse_deviation_ticks":adverse_ticks,
            "eligible_for_theoretical_preview":can_preview,
            "assumed_entry_risk_units": (math.floor(nav*.01/risk_worst) if can_preview else None),
            "warning":"Not a live order or guarantee of fill; account exposure and strategy gates not evaluated.",
        }
    return result


def run() -> None:
    try:
        OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
        archive=refs()
        candles=fetch_candles()
        ts=[x["time"] for x in candles]
        cutoff=bisect.bisect_right(ts,FROZEN_LAST)
        checks=[
            {"rr":"ALL","test":"first_h1_timestamp","actual":utc(ts[0]),
             "expected":utc(FROZEN_FIRST),"pass":ts[0]==FROZEN_FIRST},
            {"rr":"ALL","test":"frozen_cutoff_timestamp",
             "actual":utc(ts[cutoff-1]) if cutoff else "NONE",
             "expected":utc(FROZEN_LAST),"pass":bool(cutoff) and ts[cutoff-1]==FROZEN_LAST},
            {"rr":"ALL","test":"frozen_candle_count","actual":cutoff,
             "expected":FROZEN_CANDLE_COUNT,"pass":cutoff==FROZEN_CANDLE_COUNT},
        ]
        if not all(x["pass"] for x in checks):
            write_csv("parity",checks)
            raise RuntimeError("FROZEN CANDLE COVERAGE FAILED: do not trust downstream trade comparisons")
        update(state="evaluating",message="Rebuilding independent 25-bar H1 signal stream")
        history=candles[:cutoff]  # strict cutoff before computing replay/overlap
        raw,diagnostics=raw_signals(history)
        write_csv("raw_historical_signals",diagnostics)
        tests=[];all_mismatches=[];all_trades=[]
        for rr in RR_VALUES:
            label=str(rr)
            new=replay(history,raw,rr)
            c,m=compare(label,new,archive[label])
            checks+=c;all_mismatches+=m
            for x in new:all_trades.append({"candidate_rr":rr,**x})
            tests.append({"rr":rr,"raw_signals":len(raw),"accepted_closed":len(new),
                          "total_r":sum(t["result_r"] for t in new),
                          "fingerprint":fingerprint(new),"matches_archive":all(x["pass"] for x in c),
                          "last_accepted_signal":new[-1]["signal_time"] if new else None})
        write_csv("parity",checks)
        write_csv("trade_mismatches",all_mismatches)
        write_csv("rebuilt_accepted_trades",all_trades)
        write_csv("rr_summary",tests)
        if not all(x["pass"] for x in checks):
            raise RuntimeError("INDEPENDENT FROZEN PARITY FAILED; inspect parity + mismatches CSV")
        update(state="forward_read_only",message="Evaluating post-cutoff complete candles")
        forward_raw,forward_diags=raw_signals(candles)
        forward=[x for x in forward_diags if parse_dt(x["signal_start_utc"])>FROZEN_LAST]
        write_csv("post_cutoff_raw_signals",forward)
        last_candle=candles[-1]
        state={"state":"complete","parity_passed":True,"orders_supported":False,
               "live_orders_sent":0,"frozen_candles":cutoff,"all_complete_candles":len(candles),
               "frozen_raw_signals":len(raw),"rr_results":tests,
               "last_complete_h1_start":utc(last_candle["time"]),
               "last_complete_h1_close":utc(last_candle["time"]+timedelta(hours=1)),
               "post_cutoff_raw_signals":len(forward),
               "note":"Post-cutoff signals are read-only. The 2026 history has been seen during research; not untouched OOS.",
               "historical_cost_note":"Two-pip assumed adverse fill; not actual EUR/AUD spread history."}
        try:
            state["pricing"] = pricing_preview(forward[-1] if forward else None)
        except Exception as exc:
            state["pricing"]={"status":"error","error":str(exc)}
        (OUTPUT_DIR/f"{PREFIX}_summary.json").write_text(json.dumps(state,indent=2),encoding="utf-8")
        update(**state,message="Frozen parity passed; read-only results available")
    except Exception as exc:
        update(state="failed",parity_passed=False,message=str(exc),error_type=type(exc).__name__)
        (OUTPUT_DIR/f"{PREFIX}_summary.json").write_text(json.dumps({**STATUS,"traceback":traceback.format_exc()},indent=2),encoding="utf-8")
    finally:
        pack()


@app.get("/")
def root():
    return jsonify({"service":"EUR/AUD H1 LONG independent confirmation","read_only":True,
                    "status":"/euraud-h1-independent/status",
                    "results":"/euraud-h1-independent/results"})


@app.get("/euraud-h1-independent/status")
def status():
    with STATE_LOCK: return jsonify(dict(STATUS))


@app.get("/euraud-h1-independent/results")
def results():
    if not BUNDLE.is_file(): return jsonify({"status":"not_ready","details":STATUS}),425
    return send_file(BUNDLE,as_attachment=True,download_name=BUNDLE.name)


@app.get("/euraud-h1-independent/price")
def price():
    try: return jsonify(pricing_preview(None))
    except Exception as exc: return jsonify({"status":"error","error":str(exc)}),503


if __name__ == "__main__":
    threading.Thread(target=run,daemon=True,name="euraud28-independent-check").start()
    app.run(host="0.0.0.0",port=PORT,threaded=True,use_reloader=False)
