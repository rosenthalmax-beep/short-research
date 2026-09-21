#!/usr/bin/env python3
"""EUR/AUD H1 LONG #28: exact archived 27->28 portfolio-addition study.

READ ONLY. Self-contained, no OANDA calls, no trade submission, no live changes.
Uses frozen accepted trade outcomes, NOT historical candles and NOT a fresh
signal discovery run. Data cutoff is 2026-09-20T18:29:00Z. Candidate outcomes
were confirmed through 2026-09-21, with all six finalists' last trades
closed before the common cutoff.

Original 27 baseline: be25 CONTROL non-AUD_USD, exact H1-first non-hedging
re-gate (2763 -> 2672), plus accepted AUD/USD 27 three-strategy pair (357).
Portfolio parity: 3029 trades / 27 IDs / +1763.8084030796124 weighted R /
100->1731448888.7485507 / closed DD -17.089509091188603% /
conservative open-risk floor DD -17.84461250071128% / max open 6.

EUR/AUD portfolio entries are at the CLOSE of the H1 signal candle:
entry=signal_time+1 hour, exits release after exit candle closes:
exit_event_time=exit_time+1 hour. Signal outcomes from the frozen plateau ZIP.

Execution caveats: historic outcomes at assumed 2-pip adverse entry;
4-pip source numbers are standalone diagnostics, NOT a 4-pip portfolio
re-simulation. 1% of realised balance locked at entry; EUR/JPY M15 SHORT
risk 0.75%. Live sizing uses NAV and can differ. Neither interpair currency
exposure caps nor correlated-market constraints are imposed by this simulator.
"""
import base64
import bisect
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import statistics
import sys
import threading
import traceback
import zipfile
import zlib
from collections import defaultdict
from pathlib import Path

try:
    from flask import Flask, jsonify, send_file
except ImportError:
    Flask = None

PAYLOAD_SHA256 = "a4f15094006b4418859b96e60f46ac417b7170f862265516bbc54fea5e03a33a"
PAYLOAD_B85 = (
    'c-p+ZThA=Lavt_y_O&!%9eC264~m6Au^nme3pp1X3Zeu83?Z;}k-*4*PqWG9baPnjnqhapYwdS^HplLVHPywcV%1as;}8G)KmPZB`mg`|FaPw1zxv<)#~=Rb'
    'U;g#K{l9<st3RMI_Ad?mrQtss|5t4P%9#K1hrjs4Km9-d{IAVwc(4B7|M<`UZCU;6KmFaG|L6bw56l0>@_+hEc$wutmjB`x+Q0b2fB)zI<Nc98|1V(w{I~z<'
    'KmLb5tP2Z-Km6N2{lofy_`m+;AOGtI4l?CQ|N4I}Ys>%RKm31x(ZOv02(Wxmz`U%w_iDlXr@#OAZ_M}#Zueuz<sDGkO5Q;E{JMd^|C|5xcYl2E;P3zXZ~wNt'
    'h2_EKEv)etT-vU<1)u(ZYPaxj{`BwvyufP3POmEU@`*qH4X80MICjEXG5-xnkQcdvl_B$~)IG=gL-7)!yTi9V7W#rQib4nTM+j#-U;^@JIU0!0l^x*w8`yU6'
    '#W&y&j~#sjfPVy8X<&JSVi*6v`!_{t=*1k++u+c+5SIV^P@(eqv;oz7n=mOA2>bzH3qSO9UNAMjV`}swPe8OH4}UZ$n#Z)-H;>)-)E_V>Nvq9c#-THh%V*KZ'
    'a*hY%xx@S){_TJM#BBDEmrv*wBOG3~6CsZQFKPpJ@LulEAa5tENd~Pm;Rt^;GU~JnV{7t8J!L@9{1HhfXtaMs`QXImEI2jc6ugPfdxl8P;uZMq;oBeKCy)<*'
    '{7Xa6Xm&#J1z?l`Ll1C*(lH-<@2Y&{E5x8dyb4h?(vpZ<71{kTZobuoj$~9pw}Etwv$%o&A$hRa%OPor!NGFKyMP=%56R7Z>*Sv?krMs_&<b8wUUiSy!VQVW'
    'vsynpcrn4A`iA{ua;GRdIkWQ3%$V#Dv3~^dQ8YuutEbD7X8415-uM0jleeb)AcRd%OzZu9qb4gX(y57)9dP_h#KTwei~arrv(}vW!#Vyb^^R{M#>c)JA8SGT'
    '<ZozvldKKq{iSqtg+EXq&YdMzGrJ$e52<fw(04mnP=RU(NFy_zR_)k~cLU^a4R?Bu(s1LNl#oP@Yoq|TCsH;XN+E}2;`^8>xhICSMsV`5$w<_w_aWz}q|rQ~'
    '8sVUVvTCe?6amwk`NFZ7>E#@FForyM_QBHZ$&*28@uQrq%B%5yFE&5OeS=t3Y&xutU&iL3`AV-hAR4`<;^dA%$A>`E!B~w!hXbq@bV$aCtx7j$4wnsx8ub1E'
    '`~;0=Y^xWSX@5F;m^R!SD2>0pd^XD7E2VIwW4PNO#}F^^nRgV~nYTsx(I93_tb9GAobVaJzt#`TkfRQA3gMy*86L8MO_F+um}MU@uj0p>d59Of$K=-$!Ujb5'
    'W~R@QEM`8v+PNgDwuwhA=O9Dw#=Zy5;1{qrHUuP#5x1oLl$!7%em#LddKc~C`w%Wg@cqYb@TbMEeKGCGmZwOMCvMWdQFE)e-zlz;bcR#zSV&}6q1$v5WqpY3'
    '$pzjLvG&!w7E!58f}~WL?0`#_1*7Kl2?l^%`ST7hf4(BwP-)BzCl3cf$fa=d$Pvc5lqKA85y{lwoBXkBDHR*oKxr~5obH+piwcxhqh9N=cY&NB|M`FVyTATl'
    '|Mb`Y@!$Q!Ie2^c)@AkK;oWy$jT7;k`i<ps`OPYgN%x#2f(AO|N^iTc&p1#g=VU~!3^^Q9>cVTv0jzTY7a!Ly?&74Am@)As@Z%vE?4PS>c=2!pIul%QFng|k'
    '2S3P(LoSz^k0?wyTvJX!4u{uv>@iLm!VFpYUPk%8YF*VkjK617GK`><ay*m?br7HAX0M5uHz2z6X8A0l?68>A3!X)Xw`*g|!FsZn2pvHu^n`GXpY9Le@`JuW'
    'Ohd%a95{{ShLG(LvV9_FpP%yuVEKqa*E4u`XoXduD(K~l*1U>Z6HOfx{e@b1<y9-Mj=Ais;;~NXKD7wSs~o!~ZKaTSUEQ0_+s?sMxsfV+H0NMOMKWgUor5<N'
    '*JU86lrzisB6cmO^;*n72fi7FRRWH(Ca<eV4`in@4y7)qWGaY|)2e?2a(a?cLl$pRZK>~R9i$sFIvWsBy?+4mQDRfnX8i3&E~8+jlm}6`ZEJOf=+IHHmJycV'
    'Dlq%$Bu+-<lqPTSOhnh;7XN8jDzzZ3X;%hgw^B9kSEQ>pkU$0f1^|W|2xnO+=k5D%w3{(BKc6}2sYZ#PK(;MzEguMD7nNQq7^o$s&iZMGHKS5l8kZ74VpQW&'
    'Wxg?E{|{fgEG&8RS?jp-1y%SnC{W_}hth!#ey8<c=O7=8q=t~gAy3#s4yU&zS3wR3(VajR-4xee#r7>_M7P*T1aC}m{Ap%(c3zKP=e3;T552aRM$6fvvFu~|'
    'mpNV|e>MInn@QLDWcS59`B6&C0)w;3we`ULVog|%B(!e*&EVY?I{>3hQ@QmzYp?_NMaE=GQ4$U<WGB1lZmm89tq(T!ZZ^W1gB+icvK}(R!NX}FBlOz1zDSxV'
    'J(UfICVlDB<>tPksl`maw8`~tTpJFf#SAEfLCY%lysNfqaMy4d9Pq4ryMfY#M6S(7wHxT3o2?Dpb?Me~XU+sR8ML9MhvYzs-#|E&_T5)OvjI^`eNY<a+p5o-'
    '0?~|uby6mC2>BW*lj)T`Hc^DxwH3$3NtqA+H&`0;;@9+A6wHe+_xf;qm*|GWe1VxHJ>f4h$rDM=57~_|^GLS=PLrJW#b9mV{M#Z-W)CwYm7@EU(A=kUTZy~F'
    'rRSXzUvcO(8}M+Pw-U!^T&nd+ChEitLAEEp++wR+wtp`#*?{QeJh~(CZO6lHexTRil7#grksycl4h8SOdCxGkh)K>@t*eiaZe$!r51EFL-MCU3U%|rruj9$~'
    '=L`vz=#Nfni$3)7Ypn~rtFv2-_67RFhrIU?{-C8Rui_K_-v&f;MuuUbT3-DkTk<0<NYIz5g*Eg<8L)pSy_11}67!lp>^ppUHFj@^$E`YF_swlUG#+1Q3%EW('
    'gL&X8FqCsqD`znY71jbKe;Puj!o^^t;|Rv$C5jT1DXEZ3JB$@KkXGG?GjhXSW_}c_?sf8K!w9sXDA$^hghFYkQ^~n(LdcwYsq~5v451G$1ZzA^yQ~=$=<$2e'
    'oaBgp7(Xuh?A|v~5yQOyx}nn97BVB-(*Fi-tVa(ed{;@I%;*3j^Vk7IMtuN{u3q!1f1rad=r3F2+94n*`L=pL)8O_bjatW+9<RmxOo3p2kX1_A%kEzkSo*F`'
    '|K;bP8nD=Z7S|Zz2^y`~k^_z)FV5`%GLS@Mcdd+*i>(Dr^7)uoA5mi<<%t;E_S6>fnsg&`dy)JGgjCida|%fo4%U~L#|%9lcR$wId4`_YQXP%A5arU>ylQVR'
    '$&4!@An6nZ*BV(#=6I~2y^)`1dzMbjIOtkgR|N`_Yme`YBPazO<xnh^Ynr=*f{a1~7);Amw+CESQA?-syT4JMq^hV(pOu@j)2fvS8n1OlGZ59`l2J+JMP)TS'
    'gxTs!ik6Rp8R-Uy^#H%6Rh9U`=*o*SLdkwq{ABLVOco$PsobzqdUenAvfAgj*+se5q6r(fq=o5<<BCdgz~|2nUeAUE@lI6-Qoo)FGmbDDIIVHD@5^;>wlb36'
    '@0kiJ&~0GQ2$xsS=)UMh>>>s4tu-;kInTQwLOpmZg9k+o45LGg)`Y}3ZgB1hEs$7=o<(krv}${u7%fi{f~N>%n5+R~FE~(<l|&oqvrhD9%c{Ts{yNJ_<<S;U'
    'PBfF~B_hFV@2@Kmon<5=y5VVc%~svsGFNeEbOYK8-vj2nddiOwjW>>9v5IH|#%V=?mVa4Rl#GyM{CpbKr@HR<ADV7nD40+QU%nj9tKZx*{C=2%`bM~*ax&&{'
    'S~Qi&rGp1+qa!j~{n5|X9Ms*5%SY3hE9*-`Jbl|c3dJMQ_>AkQ2GM(ga?yJ7ju!rpXx-v40$%WrCQzW0Im!g^WtAT|f1st58bT&n$FWpBoE6!vErEXVx<WA4'
    '&E<m%Bzp;ZP!LtPh~9$<Oyn8-Y|Y7+_}7GY|MbtRbpODSii1=KX+v(@!5ki78my1foc_4x`$1L|-|HT|yp5qeNtYhN^?->YX236)vnH$wwmoCAK4U7%6+`9>'
    '8SbyS%Q$oqGnU}Gug=@30~#Ol_@>!62JO6VV>jfN^dvL;h&za1P=8KbJyak%&?qw%<ke^V!3@OcR0UD2frSr>aG{AFg7;{bD8f7MaFSxJU?L@acgTIU@ZD(x'
    '`XwIWh67YKaQQ3>^jT@upH|WHRgh6do>f4htR=oz#n$%8znScoSo1xvaKa*@vS~YgRIX3TtI+Rn>0DP~<CLWHQZICEMN<AOni1v>SIhO4cNNGfXtzx>?ApM('
    'O1Im<`lxdom_I5_Z_BIkivo@xee4Z};(%X23UE-^260;TW7lsnA$Io@an29ifzqi7pzuD{)o?q7;0}lqv}6UhRlvIoeJT!6DvIQ9yZ2ziM_xx=#c@T|9q3zE'
    'gVt8_t81#Yn4@Z<@Alb#DzmP-(>iY(t!H*&3EU$Ddx_YiCY$NX6h`C3R}xLHy76LHYKKGjMy4Ke*>WSF+QF;L2$G8E&Xp;ZAe#|fkI0)7B=t$~1!f*%zL^|s'
    'CmV$}V;Qy6HzIj8oaVV#VtPzH7mmE2j3#%eG!_MmCnfH{Nr~4BtABPll$e9;{h`2(bi;9zA7yl!l6n0=kaJqK<0>nD@lI6&hS_4g9W0{~d#bx}qS(RSPa$1l'
    'Sf$WWnB$vQspoQEZBMW*gB}95LlL{vIk@=Z9TkZ6Zt}9yb!LE$Hh?7bac!$-{VmG@>GvcRbWyWQMaRmGc%i4&ZDaVnK9v96J=}nR;{2p`Q1SsZ370w-=e_Hb'
    'R2omE*Sv2R*Hy6EIle!4i0*)BEFrZ~e+&Al^aHg+K!$=2vMr>C*x}rJY@7<SSib4X@FQa$(PvPN)vlm$2su6jnU&GI0`gf<Ia{&*HQ>Q1DUGta+z}!Dv6wNL'
    'FA-l%Te3Y#YcJp!ZGA$%zxATxa7xHtxL>cU@$S*WI~=;BB+uqx0dt?6`4-GkHB_sjvZ87<Wv2q6G#C@u5IIOi$9Dd0)RpVzkAhH9x8XnMV`kh&v{3N7+Lzz%'
    'nJOx;+<?T#yjz9alW6%#y4()6B{?nTaGGz0R{&H0M%aeoVWalio3b4$o!XN*<jk5K<o<WQKV!TB(SZIKad!aCD<L({nQ?`fGYXXh#Vzwr0~n9}5O;J|y3QE8'
    '5nu%QUT<5a+cRi+8fH|zjN1gF{z;ifz?|~Muwk^8AOeRTmE_lX%a6H8;~hBpCS;qsGmIPYRU4$1AGFN#Q6!hFyXH%<WxRTS3waa`pRt)hw2SQ5zs^vk$##Yc'
    '%W1d3!!7VvbS6Kaof(J9NwlRfmn6t*UpI|g8X?t4Z~Z!c%f!y4d2Gv)HkP_ix19Zh$QDZVA*<GM{KQai9gy)em;ANvZ?H7SQkJuo6(@c%LUAS=ksrl6&B-&x'
    'CC$mJtV>n>NG-j4kWN#U&x%POTPiqikCZbEqZG22kAK_Ve@mSsj=(q{9f0-GPo^d^CQ`zeDoX1rYjtGiAhOzu(w<Zs&}e()`gd4ybs2$eI|W+E^aT&>k4!D~'
    'K}zd^!=n<pR|0znV*N>p!;=!b6YsBgY&RSlgXdb__5<cTf5+c1W!GM{$WnIe96%N<#yaE(PMBm;ksX6!_TB-4g=vv*pfuLQ$=qLq4ctX(R{>e2@PnL5c7hvn'
    '^#!qZ#?SiErahwp+^=YCy;t;Zs{sQ;^UiBV6;vUEYe$1OJNR<M$j>5AC`c!AkyjzF;_03IH6}$zrcggBD@n>T((M;IuFU1r0fvJU-VyP6Gve;YgY&aa>xZ0%'
    '_``uTw#6TwIVm?Bq%zwW4%0ST%u!;Q*_}HSt#Pmfz{`IiUr~^>!P1;OUgoKHWqgA`8gdhDdc~v)b$m&l^uK`>euoC{Y66QL5T(pnb_&<k&eg*~Z5H7=3gssv'
    'glWCsKxsTg-_n{k8)yc<NlY`bPK;VMVJA&x#^jVrW9715-_FDo7`Y(6|B|tj*?nJV_jT~Sz*p(EISnTrQj66gM`kfN6w+^)zfjpwk?I8iklg{_4i>&IbCn;3'
    'I;le(>U=!a`O7dzwW1>(Oi6<o`Lw5GQHkA;bJkAZcy6e4rV@i(^pRKb{(Ry3Afvk%`WS1|zMgzK%yKc~+i|-54sO=(-Q=WDnNZ3fNkR`#%putU(VR!?bQc_m'
    '{Ma0H7cpx^qPx2aE!}T}8u)M(c~j^74hX10%c2I`o5XZ==(!<QDKrey6Je$FqX%lRG7hVQj66Ev8=}0sjNS%AbE4<ZN-b&9?2*Q$jXqKUX2KYuk`qk$v>Pg;'
    '+rhx6l?IVqpTOT_rPGr^T}o(qpZg~b_~nGLOAN|DU|{yB`IzT2gdgPsifxr|FWcLmq|}lq=f>An#Amc5DwE?IDCw9<WZE<y3zYbWJuuealS~CWRGKp<wqy4W'
    'JIJf<0inm@{V0SU+hU3qGWTeznkFQB{NefL*Zg3sa!>L&3@>jX^uo*=+>;E%DM16uWvQ`DihCd*6boB{5_FLJN25TY)N({1J0AeRhF}a$b3|;QG^c-D8^Nt$'
    '$z$8Dep@Z=hQsJEqul4Ut&+LDIW0fRC^ZEKg`IJ&*i&R{J^4jNj9t{l%%^Lkk_5^Xs(;C*l+}3qJy3a&(<2Te#NXV^{(AO&O6~-*bX=Cn1@w@RRj1|WG=&zi'
    'd*D8^pkO6nHxhBfRCA|RIK7M|F0;HEdl#BF<VY$K#mSQAR7ibwX%$WhV&^N_3kuMQxfu-K0sI7wPkEG8Vx}SHV{OW<m`RV>jqEa<w+}B-jic?4<$(T@9XPd_'
    'MjD-kd6jR^w3bJy{Obw}1S^8vYZ+*3XB-=cg|n1lj;gOG$>9dlcmOAEfZ&T>`JZ!Z6@~^f$it=bs_QvTEu|wT1-HSWalua?JRkE6WIcybqkmfUJu3J%x&f%K'
    's)O0!rV34?^3V=zZ1D$a0^_9#<boq8MIJ^XNA6{*8d`O7zD}}n-c25%D<oCdbfUaBvT0u#{o%a20IZ2j6df*>#!glj6ii+TU#_UBtG8R2{2-%LadFm^E7Kaz'
    'I8Ty=dH1qekCV<gD5p+keU9So-?B>PfvK()hsqhpWIjd6xW;-$uOq)V92F$03(JBFMyMe?5cO>G(Fxkik>$!gX2R`}CAOoGhk%2a;mO48XwHgzVAqpLAxszd'
    '6jY?u68XXvG7pwHX9mbre(RKZ@>i&g^7a*0ZnVL>3Gj?L$vBke1mxP^GWdW`F9IDHaFHXVs<7FMz=DaC@Z~b4JOsh;{d0A9NHlJF)?^;)`Tgxv<v~h0XU-88'
    '<c!d}aBYd@cC#j+=U9Dv6<A@BO6Yj3=xw)PAGv9$IFy*Z$dX(r`OR4D7qG+cfSiK%E5$hq*@@li%|rI;lN8B`{8>7foE9^EG0}JqGo_@Z9&@*Kt*-j<@OgZ>'
    'o#|d;H9vV#+!NM#3w#Ks+b!6GFz%Udw8KGK%rH=ID^a>!<`-5J=l4(>r2c8+3un7feth7Ivc{9e$N)Z?x%Qq4igfr>06oBOdLiar9gY={7P~t{HZ694deSGK'
    '{GDuO>!*(WiplHX<IpVj6U|psuh&Ou?IN;}!m6w8-ix2@If|^65-{KF4t|Z;CcTi3p6ub=+;O+Vp*w4u^1@Hd9Y)1Fq--_m!2HoD6!+9s9v|=*osJoY?sOEZ'
    '9M{!!cM%_vvZ}~2V=_W1Y(*T-TDoXIM-M2>T})L}L1pl8c)+93h<UX~g8CC3OAQ1`fP)kv>^zcSEVE~C0(e8#O=Z1zpg<?_Afz+%YL_PGMwcW5GCJYs`O1>;'
    'GpqCM9WOg1I*9`dH<@+yW|rjo9IcpHpsTE_UyQLfKL(T=42|tx(>VLgL<1RpkKN+h>!%$IomwCW#nKyO{rr@leZ38_N-c85BM54d_&4YYwdfUIiZb@{0g3G}'
    '<GYUZP5%@^UI!o;6yCkOdNMI;Il4o9&wJTOeP&{q4VC6q%p)8mnNyo#yG!O7hF0eGu)7wqmCAcA9C)?Ep|Ku$euI&v9Hd#k`<m$BV~Ex|ARE|CXGYcyASq=Y'
    '<m>`{H`DUh`=*M+>mWDNzZ=L6?{A+tmI>c~-6ry>4K%$U!ws-7bIiXdOx%9|zC)t3ClG{AE??N^0mpa4i8o|7*(f4UUJmbI>0Z{w{v(K6z||usbF+7{!B9dD'
    'LJ9J=3UqhX!%jFV^+xFm-oL5OPfcR<xWr`$0ex`u77<l9@P1`FC*i_uTZQlysWVJAbnQTCUQ}Kwd?f#HBXS3Oe{{AXF*?*zQ^UFn-8!o46`hq$4Zcv!i|;14'
    'oU{EKZ3u^I6TU8>&+tIG6L&ZV+MPIwk9t?n9(Nfmpg36?zC)$4le{(zqP&B6+Am5k0!Qd983&0<?2;V>6}QEPv0sO>uA-4fWnKT;LR8hER9s$s2hf}*Yw;D{'
    'Tu{V(!38pxrcbuo9R$(d<8e~QO@z-oAUYeLE?Q)3DR)BbIcIle?V-o>^W{}a2Bo`Uvuh+PIq~aiXCm{}{U;Sj(1NBRX!>eu;Ex%^j6xM@*wPTxw<)$vBPH{I'
    'lzW{?l9UnTJgqJHt^1{npv=?SmFvXIfTKhny1V}I!63x)JGvP{zY59e(B~5MzWsBBt&s0d*={gMr6FVl3dIbW4@FHvQ;%_xx1TtD!<jLG(vFakZ`)S<V9rZA'
    'EcvyPhN7wC%YIMP*)}kfRmQdVbS-|@qv5*VS9dLy<`A-z-_ey;{w%k+`0ADdyZYPD$8{U^x<+eUThrWZ0pPR~?;@!K9KPR?)N?O72)3nRDY@_bacxulP|j%N'
    '{KdM8=JvaA*N$kErR>{5QwPAlT<s4nIK4bt^qhY?6YZ=$-#}?3zR}N)H*kNIN}mRIgjqU+B<kwRuUE9pz4fCcBaYA553je{natUWijnFz&6#HfF_fZHx9E59'
    '(}gt~DxI1%Hm==)>y^1(?p<v#NC%nZDmO2rwOa2a*!q`@pI37rWyzZ#7U}si-tUwi{LMLs-t%NI;q?`Jel0iv8NNSwnYUen`nEG<pfRCbhxajucA`&T<lqdE'
    'x9R-B#UT%qbQl$uDhLW~Ox^B)zA6y^**1oDqT~>=m&+tWVaPj+Iz7JsI6p@dP(iU0e~@oMGkzTVhKq7Qcqg5&$T@GQG=?&?x8v;&#1V+Qi)f4?SYsF0YcVvI'
    'rthIcnSZJK2BPep=hg5;cIZP?L!pC_y_m3`6Ax4eXBemxGq9XB3_{)E)dgl5MNk6<S+D!OrLAR&??1s@VOi95lc6-FN_q|5X^7V!p`qIxE2{ON0DE0U^MsOU'
    '3$QhS@pjs-Z!sgty@H{1DxFZB0wD$$qqVR&hhaMC+(7A0oQ5o`vw_ch+N+y+cR-Y&y?jlciNa4*+rPkv;3I)aR8p>_$9?CnO0PBv73kE4Ko0ezW=a*NiMG~T'
    '7mxzN^4}L6^aRStKbColByGS+{P<nA*`EXF4Tt7BJg%Ok6NTO|BHcrLRdHCIm_@qCyb2nATo)pzD<F%A({)c#$sOqn<+6AWwDLfAsuBf(&s6knKs2@`wv&$<'
    '(9Oifo9+r@JDq7L<w0{>Y^y15RBO<7x8;xeA>?qz&&9^qzja{T&&lx&$?4D|$<=UiFokMYDS^8v*&^5rN^dt@My2I><nw3`enQK?0nIAn$oyF=-bxg@V?4W+'
    'h>(uV=zOZD41P8Zx?xh{4@1+kR`!#*m(0Hc@tRCr602MrpH~s@4kHQ*O{_WMWZ!AYS?X|`g=~Ta^Jf41IjB%`wEIF{Ed@fK<jv>wSKhkG>_ovtI{3}R=@!22'
    '6=F4%Z^rCtnPA!nsHhY#YWl1fU(`g;+a2$GUngw=<dxEo2VbX^e(bfGU2oYd5KtMJeBiL4GRR-dW|;$>fsjf|8m@x|G_AzxG&VG3BbA}C%a5l^wh#}e<ZugQ'
    '#OS}Q2CeUiP3)`><0=lF*c<YLuGp37+KH0j0z`L;4(Z1V74ms~Rzzbl1_Oi+c(y(ZHZC%SRe{puA>wzEqIA4~i6lz_;`Ny0Nap6Wh%)f*k0U=<MfoYm6Zlfj'
    'Ton1mfoMTtlz_eLwydkDZv6Z?K~Z3k5^+2b9yEyM;TBhxQdAfk*92Pf(-tyH)gRX;$;+dZVit#(ZL4rU$x(sf)rgT~#H8rMLK8STQ<|8rYr`t&P`MEjFO<th'
    '&Z*u)u*f!=H0Z*I3%&=^-6t0!cOipVR`Cg1n2LmS@2HalASkn3z}_9_S0FmWB93Cqt8_XDIkmg7;b?>N-f=XONx6baX=9K^hdsK74j&}-`HnKrLs_M9!&P!m'
    '2*{&dgpFXJ>z#b5=eQC*=$u;gA|FUv4Lt|z2D^@4#|M%zffwJ*v>NqV@^fci1)_U1Nv<BxZ0Pro?5j95_f~ldgAsV9%4)oyim~F*SnW&|7ez4}r>K4ld&)f%'
    'XD?L>sGyX&Po5!3_<fIc{&KeH`Xo*M0n4-`BV#_}y19ZYrefBLS*jNMoQ!cVWL3FX{wSghIi8-4_(C$?#r&JMa*0{|2s-?!J;=xg5PB20?(hP)SsJX;ZIB6c'
    'VdL66n;Sxj<2sO!Ap~NYHsaWp4i3f6hi|CefRpO6uGaK~!zdvK*@?yVj@O*l)fn{KPl|c2*g#OavDgmBZZ?2x6Z;tn=@P8D#S^DRb6kkC>4^=K?hwuO5h8wf'
    'dd0aYw_iSO%b)0Ht0pU`OEzF8o3QS|Jh<^(SI?La=!N?qo@Xj4s1%K;VVjE4%@A#?fHy~Pp)({zcu~W9pae;Xsdpw_>}`e4yp5rhQ=^p^r3~RT54TvyC9uaB'
    'Z1fOmGcG&hxQpG+)g*Mr4UA;o^gK2An=Y;KyEz3pkLA%q?#FI*)t`x+ys3Yo*U|CQ1{9H8{%AN*FSi>AhceJUEsVwJB5`mw8gw}8i0xqM)IJy1bb|h{k+Pe!'
    'f)GM@ckl(6G><z}rx}2-@vgJD12U&mis2Pt&y?um+@<Xfm1YMhcM1oY4gP~+GdgR*BSYd#;5o;vJ{L9b0hW?xaRctO=<f=K&H5~>I#&ndB17S4KO?@O7qAcH'
    'GDQVBo}DUQVX;o`BHzQd+Nt@wI+wl!qSN-~f~W;e59A2yla%@m(gfGA5%Bw`Afpzt6kyj?e!v~jhSWV!9mqEi3<z)feHGvoh6E9Cjz>@XuwwuqT!OkfDQeDq'
    'Bv4+7|2QS4#UE2<;qHdi9T1JfLZzs-Z}VvN_}#e9nHQ%q8bY+eI3|l196Z$4`i{3iLhdN9iZ$|AmMK&qMx!gVCV``%D^!lsyNeKaK$N=TWs5@X_5VGWLlp-p'
    '<s1dt?+h7w-FuCC^Lq>ea&*_IcT~}SZy?3fl3JSFzB@~vAOtok$AxbC<jj~*_YH{UEt}E^?aUXp3{@Oj$ZiPPf2HS>MK~3Z7P~*#pcZ>Lo#VP3q6BYp!6gW~'
    '!ZgD1`hK{BFp_@1DXz%^xRj1Y<H8cfIgA%%0XC+7R=u)K>bA25|ICSmMj8Cj+R<ft$>_Ywy#d$PCmA;!q&oSVQI;LdpULQ}8_MgWtX4UsQsKPnZg2XmD1uHi'
    '$Q%YfG=Rw*IuvhAM{GzmULnhzNZTsl13@M$Igu(T5+Q>*WQBypm0!p3U<fNM|26n^aXq(f?H&B_{+pE#K@AauY^Zt751caer5vqw2E*bc20)s2RWd48ZomuN'
    'li@wY;ej|}1EPC1A;@<_yfTV}b4CiFP<|rN6^af)c{#)@>jwyDB^UPU`!D1+9HbID#*sbDja)+(jYYUVTHgL_ovOc5YE)2Ob#C%{^&p*_R+tEPhe0Q!DU;hl'
    '?!TRROu%PCxhcy@xhz<WI<RLzo_AJ01UV48ZEs839eAlw@iI2yib7=&ZK3~ULF73}ebTodjPcPew%qe#Tju)qcbG^e@KNZn()ybRsuQd}aIl2|s*l*OnSv97'
    'B>UQBSRCO-2mf0Of5jBk@WmLyrmg@JPhEh3R9Sk1oAv-QD67Pehui56e%4QgoAAyzBs#m&ggGs0cgneXO@y2u1xiVKVa!xr4c+pDD=CI{Ks4DyD^K7H+jI|p'
    'GM8nQqxdw~!zq9ES?W+{OSFL9Fy?eZE5hZ*+v!8j^`qjwrfa*J1=6p@DKwa=U>$O}27lD!C&Bu;wxPIR^R(D)XKbMLUuP$~^>?T=mM~*<@;7i^F?5Y3bgip5'
    'lzNbp%-iY{XU_U8h^hc0<3d?iqSSS`pE|}nu9ygOT|+xiN>z#S{hM3U9^t&E5+Hh_i0ll7>52rWB<sltqnzK$BSYYnifuRg7_GOHp@l02=W6%^@y9R7K&B)P'
    'T8aDB4|ame=T)zz&YW>sHXs@`U%C5{WY9tKXs^w#Y&bNSL*d%!hn=C=_#rp%d@2792dOXv%4LQ@ph~!!XQcvBojjqfj8kF0(VhJe;s`rR1W0u?+|Glz1EN!s'
    'Q9dFR3vz$WN6;rE!WxGL8L~aSzE8)DbIUmifz0C9cGv}e*nqa*WBsTY97`LxzGNb_<a0yFgR?QO+WRA)&`ztA5yv(ADpifewG9YPBOkkK!48gR@n=v*iJyc<'
    ')ot}GZs1q&KKez^4X58UePpg;)AX!Xh%9e_Q<j~^uPKE$p<RrY=Ck9m#BRzhSo!uXmOg}&4n$`dBeM?{y)$91191mR<F>?gUJMCG(z<$9BfNxf`<mOh>5%zd'
    'g@TGkA%8Z?r9!ctwQ>hvY(;^(mli-yx8zm4J);%dVVgt1EVT3IRog2kyFTWLZGZR>vXvW{@~XP-eMXNP*$nj<>`*Q27v1kOrw>@YUU-2-bz24hM61jOL}xBE'
    'pzmh$`~~#DPXVu(PRHa4ZA^@wNY?OV@mPMUQ7ekr)RBiJ_$Mr$S9WoHK>7p>@QlR^%D^)7!bcG@I#oT*;y*DGv5Aggx=bR23QGJ@PM+k|I39?8N%CaFp?ED5'
    'N4XPI;F-bP#M4}+TSRSLnw)27E|Hl5sVc0`v4_(-!wZg^FA)bZRV7OM@+?HbVReu*b$-_kDXV<<7`M=&@E<C5!_nDdro$U8k~XAiSA-N9y|J^Fusl~QbSF@d'
    'uB!;Ay#w<BK|x`3fb+;4NlVa3^_zQQMtX<95&a}%;0~2eXHs5E=&&lQCsc5Ylf9fdkh{Jux#>Ja1*NEvC?giUtTQA9k(XxM3J#~#8Wf|l4B5aF20Xz4C1M&K'
    'If=f99(Xa4)F;j_fbS6T7l`HS?L4XBoIS-wpx_oCkX;C2zHO?aQks%Dl6j&4F}Kf$+<@q+3hLkrl0l!R&_Y~}%fQ6o$OBI19x5h{`GZl-QeG7Zq<8g?3u?mw'
    'mcz#b0P$>LctNEq3{Ris!x_`=fTerPeHNC`gEJf{pr>f3{jLp_CYZ<=S^f^VmzcQz2+B{=T%V{QyZfv0zl7{$Uu#|MWb2zuN<cJ@jdgKR66{une{&aGK^dKz'
    'cwrtxD3cxV^vcRFAI1RI6UWG4XHTe)*(ReepqV<};0HK0l}pULzwlx2=Tcb(6X@Wh5o!3!s=vQOw&KuP_vEM;QRmaD)hEMeFtf5YJaeq0tR_u$(0$c~<yoLS'
    'g;F`Pgc{q6U7_lLad8DWO`)?$Je{w|ptRZ@<k&ysKVgH#S8`(@z;cWl7Y(`jP&C7`%CCTqjLDrX@FOp1#-Ta-S5(q9exYRN)A8aVf{hr_P-@JZuE?r8Q0UD{'
    'dq2_}eu7T5z_3chR`z&TqPp{rpUAxUxyA<s#9iUR&%PHbhmBROfG>6C4v|~kEf5ST8S^Td2lBUi62`<tZAW^Y2J1L|73!~{T!x{ue=DdxTVn@u9GmdO<;AJ!'
    '%{WWe2e{xA-C*g=;o!P3lbh>@L{7bp+6%+NxGuDX<<&=M-zpGLsfswlXj_HwK<#e<5_E#j*K;7KcfKaPzviF<F)D?owg0sPI&LK3YG-!Fk=lTqg3hq45ZrI8'
    '_fKP}IL_UW^`qjf!n{hFqF?JHQ^dvhCP#7ZxT&D@`1^*h;&=BZ$SVr31WbYxl2^Y;0R8BMR2W8ySYnIHt9JTq(E(<Sqq;%r*WTw<*K1h2*lVgdSPi+K!mq2O'
    '?F{_c+*Qnq6#gd3v78mx-rkczTtZ(A4mv}NzPk3V&(%sj3fJD-DxS_$jMi}~0wP0z&htlZb61a|-9Xe_gV&FS(FTC@4W&O3byOH$sWeC<$jO89kG!0yYW9w0'
    '6hog3HLtous_?GF+71cm1nq<=IdzrBBsv$|3hscA(So)z=<pSr;!n}z4#p*r{Z}{MGp741&|ZiMw$(i&w@^Rh3{cF!eRP-Xz^ej;_?|8@(3JiD?ix53JL6Z*'
    'P!EnwGE$4${>1UdZp=Bxx(f~*4C)iJp-69~9uFFL&(P2v4xOfu8J##}dS$YL|N84Jmii>!DTysD+X3^odUG##eUj5+W;y!JdD@#f&ngO4>^R7-)>ZdFxUS8v'
    '=8s0<+M}+jWjtXr)@(nL9<mp_r@R_&r`p^B0hKO;5sDjQ;k1VMfR~zaIGvnFkg??sm5vtgV&Fg=T_6)KP?T>rfO2MJ9Xg}9U$O=;72%;@A~H8gDOXV$<pxN0'
    's<5=94HK=YGnYJ?1v?U2XYFL#bq7mxR$^PW+iU^(l&fV8>5bzhriU~RH%v$L8I;mKqQob%t%5skFENH0mt6#(NPn*Q22jT?vf36SG5p0c0OPDFDKU$&qmX}l'
    'd=o4Fnk}Suvby{YP@8`Fw4BC2a6)Aqq=Sqs=fFuQ_PMxqtv=}s$bJO*jKA$d$W9Ge%+#;E`kjJ@k0zPuyzyYHhu|Y$vjJV^p4W;!>CMcm_Aw5f`bf}YcY<By'
    'b}&%?Y$b0N*P#50pJQdeW4GF&(%8k7C|&e?^J+L9a_SsLiWohhL%1OL=Q8s~6Cm;08zC!@+{|&UY0Nm3dV?(c1DRhsocd&oD`}a@kdNL@dt@0CD&YtDFy(Uf'
    'eNs6a7AN5S6WwI-a)kvdrLk>1+kOMA@n|R-EWf;`$q?JN#cA>vVC9jlapOS`Go#woh@GbYbp7ZOu=Lx3@+wGu9s715Tx3nw1i$$5=q|7`8!U|zzk(E<;LPQI'
    'GHae+!8*2)$N-bIIGa)5Fp(DjK;`>22_GtN!G<LoWH#Q~X_ap%W`+u6Wk1KZbH?tv;Lk9;60tu-rX9qjFA(~z^=F4ery~vjc!L>zZ~o@Nu<#3(Pjj^E|17{9'
    '5kduJly<$L$8I-pWOKSv=*>OK8D=rIvn`37K$zhWO-gTg8-R$X)2FuF3Mw>xWepau0x~7<6O|zuhu1*{BYW-=dGXcNdpjVSx6>7NPZ2a8hPd|KMgw5B*$|2F'
    'WBLMSL#5n-m(L)#rZM9Ej!TAtN<|4Lh#}s7{gR)f1ssJQtGWtzPlMXwFe<f3WCJV;8A(B3vvbCSXSUNKPneYU4@ve6U5P;9<kaMVHreiI7awcugq%1wH&nX!'
    'G6jDi+$Om;T)nTVWdowQO>(8!rmr==^oZR$y0W5BzM3gA9;kWjC2?=Ce(6Cf&Guy|*hs{DI_=pm@ml#6qVdzNiEqhrhZ_h28ChP9QX%u<^2kME=ZT!N5ho2x'
    'h7^?OqtGK>S0C|GHXs^f?$<e$@@tt_)x|6m(S3dvs+@423ZX^cDzAnoEZ_!&%mturZ}Q_U`tc=b1X(}U>h8OHP%03kR1X|fJFTmD9-|>xWar2wTSyn!!5%S|'
    'U`g`1D&ptUYoaTT9^YP}4G|BDu!C^7@^NFE#t$zpVfv5Av)Lct9?>KC1ae7YD1<U6L^edYID;WraGvc2S3?;Ut5iOV*o!+*%=fuMWikxaU1(@0B)5pUM{;<1'
    'j&OycV9BbO+z|02G6q&A^7BW%aOAA3!bH>E-4FVbfLKdz{d^JHhDvk(gyEHEUbW*QP<{mygN<N0Vzgm8SPoy@cZT*-wu3+F5B;B0oEr|%Vg^~y(I{>V(ExB{'
    'v=}@x@Z^xW3QMW?&B&;B2XK0TE-}8dHgd3*j3o^7uWiC~8T>~~<2Q&0UB98ds1WFxY)W^A#n5mfftd{TGbB=?AJm)WRla?WVoy*q=2+_A`_X2eL1UkYCSqHr'
    'Kd>yZ+?w~T;kL^6M-$tlH0Cf4s}J(3ec-|*>Prq{JCL%493O)mmDO46#1*lcFAnZ_E@bbhPjY(5VF)?i&fc+;9jmlCr*tH{0W39gUJsg7BNAh0gi8zBY6h)4'
    '>pcWO6O6TRg{JT)D~6;gLG}XkU<kf@O2H0?&g>=j8Cm8ji2~2OkU=eGghR7;3mJO_5l#NsdR$B1*<cJFrakWsluqX8<(N3OLq%_td1`)*I+<r`Wfts7T0PCR'
    'EfMA->hc@VU2(uSjL)*v(_{t^U8%f!=Dc2F5-Sj+1uc0cW!2q%)m@&W@k!E0QS?d5YIv}8ygU-r*rohnTJ^oVz^enY0)v%^otO_Sh~wSYtp$bC0S397Bd<=q'
    'gj&F$?Bz%wwPMitImhF!_LG7`sm9=jGCm*nbVajod6HJjL3CI1R2HF%>f-y!ibQ9e6E0ad@+#k5)>EFODzY!5Kcb8|OcTlL5UV)-;&9AHUbVequg3F0@SY)('
    '<-kj9z!&H?D(@50N1a&ynpeM}0lMNuM5kk2N+0zCU-G6c=A18d_X7^+$$=^?o!d8HPbsVL%u5>duIiv-I`Qd4FF<Ii3YS3G07A5oCK80%G*8`uLT@reN$8o='
    '(r7b)Q>wBd(TE)i!7@mwokUc9afAR}re_hf+=ZM+KA8J?TH|{>hE6@AFejm|+WUEGDh@@^o<9rnJK5wDh5kUa<Ozrg13Bj>kI{@t`9?MXJMjH8YLXshm6&0O'
    '**!rZT3~1qAB}vASR{_W+K=6k=u|rm5wxP5`}6HS?ooYoA&*`OfRp#A1rzIp-XtkM;hP6im<5OGZOk(rgd#y$si^P?CcO$ob36`NuQDHcr<AAlO4f>FkLz!k'
    'CQt@F305J>2N}PW+D5JneXyP6J_PK9v1N7D>p4#+yr>O{PEEke&TC$E=RLVw7d^W$U(8b;tEwwa<n1V4_~lhNso<p-VN8Qo);|`=P2a#5Sm)P&YqnPGE3T_('
    'P8moNc|-`_S_8q+R0``2>NJa7K1%(}N?nC;R%im&qp`>aJnzS6|5mv1h6TzU2zkS@8hd%eOMRC&3?<@p;sz0M$}!QcR}F6zUI|!Wj;yO^nW1Y$oG%=8h`CRT'
    'QdaGur}`%M@QnnN#zeZVEUQ6LC<xwycg8v*AKug`Gw-76^0It14izQ#;)t6vaBo0Ll}Xj%J*K_7x~K5C$p_dYIZH~sarN>)?^U>&)OC-86qvIs90e`GZ|{eH'
    '*X#@=XtX1f*0P8h+*d?hKY8JA9CQU_Ly|Q)bnTb`%uB>c!33O2=YNl%`XT;%6>bml_ej91l$-ia`CjVuAZoq{b-0;EY>!#19$-e0!Jl*gA*HhP#5V7};c3_V'
    '<Fy)X0EBUq?^U?@_P9JrGhniO6b~e0EoAJ~!L|iqXd@Fvhv=5bho&mR_Na7cPqNfe-a&Wpw3rb8tr=|(L=75A))ap~L-!j8DmCR4a=5vg<9o=Vp9Fueo@A}y'
    '<>QXpFS47uIe9<VDy=x;=)^>^hhcY#%+Uwqmr&j^d9;@tL>U&Sw;=n54K2xyepZv38tDzLlm>m1LZzbb<_hpV5RFNpl+86#4uXWi3T=b2rN9o>--v!TzR#Fc'
    '83|fxB#aC{XgI~$a=_r6;Y4sTZEL6<SA2%w32!QuHV@X_%ksoAv;on)oB6X|h>)p_k+^qI%N~bO>3MUd*kA_us&i*PBas2IPDudz*Fa?zpE>ra&2bMQ+x}pV'
    '_v$H08;5AaR>#B}85A~0&=+UuhDxX860g#Gm7hpXkd9g@f0T}Qiw$N1MO!*V@F~P!|7ACS9Brs{QYRQ4M+9FT`&YcC1j@6B*D7+!>*92%;R)y5B7I}g5m+B0'
    '2aa}L{2MHd__^gWZngl>>rHAdoYBN+>~fkWI%tc0-ItN!vGU7shmxlD%Wx9&$hBt_ciEvu=^t-uTg<+f4{ez(IZ{9mi!+m=>-oTAL#5n-FoLe=5A~+wUVcH8'
    'VJP(^VPa9{K&N^_uMpoq0XA;8DXqz<bTV(|P`5ka(}~23*>T1LW3cJBzw(Vu;H>hSu=1S|w?!P{4LU4=YxLHec}JcLcbE>n>r?EN)hM;=UF<G!I~sX}8DV5^'
    '@W}LjOHt?^hfdqk$qoV+o(H;@q3#VtR~*8_>(D&%z@JvKGC2pSK!lb9mZpm5-k?;<{V@rmgFil@+_oi!7-DvE(3Ds0?Zp*VSEBv?s1wph>uSe+ydfjK0nwz7'
    '@{|pfLC0R%li+4G%KTMsaRZ_R9s6TA@@guV#z(H^HY7%c8bA)IKW1Q@>BrAO^oBzdPuSu=N$LS~D2Wl`iR_iEzp36m&)Dt_mQGpVIApusg6NcASjPVbf;v-@'
    'C*g@=50a`}$YHbxq7?M-kc4>36?us_lAq*M2fJN4C}hldUiEp&?N-L2Jj=<9j32@M%=mj8I@8xAVGfvGTfHMxu#jcketA26U2aQSOCfXDdU!4Vb@l#)xhsC0'
    'Mzvi#0I<dEdtv3Z1dHz{87=0OfU9r!oZyYL$4vxRe*({+HXAC95!?o$fUpBFr?pxa6(71TU)KP&SbCjT`2jPSp9H#BvYC0-LWcWUCf+!-ka=VUp*xrLN+ypt'
    '3Z>ZtFK4FmoSW13B}l_qt;s)VF3cvoLUOCxNi!w7*-1l9aHF=QCxIIBK*5o};3WAy73*~KK;M|`5b^2!o4}Exx6zOTz-=7m#HFWe2RNT>K)f<^rP<)_l!Mca'
    'FfAVqa?_6+P?PK>?v@ucto=TL!bUGYC-I={`SC3s*I@&GO@%&i-;iNgC1O4hX*7say^VRm{oZhBqIkdFVWB8x<4zgI(zP;MaA`QQkL2+>GoGKXR2OM<+<VpE'
    'JQs0~L*o&yp9LCW?&L9Vs4Cll=s>5QG7>eW>3b9U+uv1XHj<s`Z+|}(b$WszUt{Lgc)KC@Eg$(1u$KeOyc!-TA<s`zIftR`B{)D1ZT4O;D#0QU1H0uH`EZ?8'
    '*=CcdcG5&;%}!EGVxG7i`ItsB#0-6orn(AzHvS<7jK-GV&P|_58z_y13CS9X=jb|o<eP3^^K+cM0nr?!VNET{nu4a6OTKi_$s2}JPw+U1szIE{U|~#%kUT^~'
    'WFe-B56<>y+<%e9`k09@lsGy$W)ABpXM1+@%Is}0F8MD&_T3!x^A$=)rOU%*8MC;9CNnC{MRtqV1uleO5+BI4p${$pu&hF{y#hUKAeQvr-G^yxxx|Kmz1)wL'
    'SNVa~gB3E89x|Lj=G&{?;up!<m0ccXz3B72Hi}(qmX_bv`f0t+pp<F?LpAAlHeK&?#kRw01R0H79~|33diUK6Za_e#C%F!(e*|Un`O~?=#yd|~{`WqDej@ze'
    'gVLNqF<E<*6$SkxnYa~(ioRj34if#8yn6d6`!|lDhAjDm>vi2%7V!6)j*3K~+Xa`E1#NplP&<DkGWeK(M?({AS$k3GWFAIp0(koz`!@`wmZZxIkP-7~Yv&{('
    '+50(03D_Qr_gldBQ1mnw@ec6b+$puXN1`TaHCWN(OQg0&N?BcL@IHy;M`A`8BIaqLI;E5m)Q~d~O*ewiB)pwOa#Jqt92jtsBUrS~`z!P$!&1cX2_%UPvxRLG'
    '?w<3s2ciTW#8g}1x4k`_DJV3Fwj#$U0&Y?gLM}G4LxXmSA(<IxO*W{D)X(96H*n+U?AoQ6!xuEVzlR22LXIM3Kw0H+nd71kY#p#6Yl61`7dTTJER8d@66SSB'
    'T9j4nS;RYBzzvB}*^_jo??hd}E`7}{`kfwSb;{BdJhYhIX-<k>E)h&bgVB<WY-dmVj7h2MWJK<lyt=<|yJc$J9*5@G$$UaWVHj-0Ob@WqRU}4-+8tiH7PUJq'
    'p5~WD4+K8oq9u+mAR?)-lw0s({xz?jU9By;jscB@^*1`4)%A=?lkzDa6;eKB)jttHVB6d}mU$5G1{1R|o%Y_-EA;_725Ta(Udz0N%&@#p>?!te@(!F-3bYUG'
    '15+g9$OZ}+rlX4t$|~{u{$P&xYP@?2@*arpRc$&J8_?J#;Aw=O=!+A{p|y+<Y4)zCg3=lJOnM}40KcfCoj8~_9BRx`X{3nx@?bwd2z0ugKI&z}k2I-QT5~nx'
    'I7IBu>huUqL?#z~ngj191JDd9DA7BS<~prDbDvglcr|3{ZmK|bkEn=}w60*14L16NVY#glvjR~XY?&@1t;Q$##0w18i8+tImBdU&;&{8>4bMn)awZT9IP>aR'
    'J|dOlf#K2`!yd2Mf@+H5n$2vdj=Z-}S5I5O$Pos>wVMFKTzaKG_5AhBAC+heU>mPMw|9|NAYP5R-$7qj@12DFeh%pXgH*OzSN+{x8udx6gzSa-q`VrWIN@@='
    '0IzB5*<tS3z`7c=mnFY8>h@$eG#?NJp2fQAwKFN3(lz~6!wv|jY#vGzP4eo|IgqA=MUU*gag5dI{g}D0m_Vl|=!6)!!Z#1t4|JNKcl{_y^H`uvHn@MV;~R+o'
    '2D9`sHq?9f^po^tH`MDB%;u2^8$Y3rL+x;PhcP1Nu5zGUW52C-E?Aqm5e-H5<N_lY-)V^cE~isrd6nOnbHsx!xYNPBmq<P1@JgvWSy6D%Ie)Pk`H<ZRGtdZg'
    'Q%P|rW}t5{Bg&ZRDb)Z~i);QUk6uX7w78<D;9_E`rsb`4SG<q8v0V2)0?R=GYFPRfpM(u8=w15963zM~858Ng05l_Wf~J>oxI<^qsLctQ(W{B583Kyv!V<nC'
    'c@w=27i~cKp60%8w!#Kja#5~6?l&Z4G>YCd&9$igsix2i2!Oq1lbWO)ULkd_R~tW6oA8Xg@*&3)$a{{vPmv?uU%;_LVswfU1cI<_Rbdgy0J#|4@-NKIa(-h{'
    'Wu{0T!nUpM<K}y4<tmP#I)paNeMQ+;@4nRT32zK~;9{*aFTmVg6jpHrrEZYG6*ho;#|yb3Q3{%e8qK}(o<m5Cujrj;hTsf42Y#}~6yAeYl={tbB9jEZ*lP>x'
    '>w{RSDeh4A+ag};Rd`IJ9qX_M!sxHj8h6m&vCO3_KiEcJoRlj0mvJ_5z_lN3@zLkQp=}4Mub_#X+6~Y%u5EsjS3~Z1M%7ig`Tnmbn-St%*(vEc;v@5fcSv-)'
    'oq_~ALapyWoASUnBudnFq#b=yi;J6DHyjX^mOEhv8FTo8mSF0TZ)7)AX+N?XjVF0u>WB2f4Gn`EYCZBBZJj!}p_OiZpcyfpFT*H!0UDk643W~nGJrWMg$l~3'
    'RGVA3`mg~m<Lz#)n%?06E#^LbMO_VN3Gf}ine&!-j{a$fO5@COaePIouSw1)4jgWSfiY^9-%oD&#CoXhi_PMIgLmwW-P}R@>X{dv%vt%S@~~E8oh@VNiRf~h'
    'Qlxx4y?j@kS6OStXQb?vx5FQh8o+>3Ma({}>2@Sx#_;4_amGYC_=&gG$nfETqQ(t}&UfW@M8K!SJ6t|@7;rceIt<ks2H7bV#x{P`(UW1b9GcFfML$<ZzuSOT'
    '<{+EzJW!Euo(a1Hq7*tGPL&q4mCB9x<AbfSXmUgkZywgE0|<^3592M^q27f1YrHEAt)@s6e%tCfcRB0^k)`{=E6j_E(60xvvjKP_j>wpmCK0V%qLfEG!TpK0'
    '48y7siveavY@SeTb$rQ%6S8kjY?@BlnHJns?#3n9iP8Ir9N9RY<n7zR(peLXaz56s<t)#rNmS-zZ3;2OY-ScZJm8*W9H@j$vU{?wev?}JIo)I1UfO|TSb2JS'
    'UA@0eu0Bc^D%f_Ydkfm04vf5T#%_J2Q7VZQx^cHvFwbKrCH5eP@^-)+a&;)bA<?Ng7`gGnwqej6Qzt44r5(wHtRi4KP;cH=5u0(K@|A=^s@twyh>zr8S0Fmy'
    '=>yd#W`Jj3MNkpJLz$)4rwV~>YT3;3_{E!;foM{JD=aZATgR5cQ}!bxAr<O|)wn^8=9K6AB2NU(CxMxu9b&s}UZqp*NMocdZ-0=;U$FsHW-v7y@MBGJuShX)'
    'bhx<NhPFZUbGGH14cL)ih2rqmE3=syg;xTOBZ-rF>d;9zg7VN=K8rBIjP%T*gHahw+O{*M#SFcRzqZ)UIBPw5Z#aZ#XT!3L$tmH3eAnvQYO=cm$qYlK*lrCK'
    'ql_4(vX~2*5i5?MhTNY?oLA|q8G66ot!^+hhvW)Mbh7iRec+JHPeP@X2QF(~#b+REJys3mM-1L~n7zOdysn~G_#*7m%=lm<SPqWp3ZZ)qNpSvXINU3>LKidf'
    '7vF$vKr}&b9u+FTkMIpe%tRTF>&^!k<O~usMP215ZkjeAMrHT%#2U#|gOvty)zf7;1)~X`EwTp=RBPwEh3H@GASf+282JVvuZH_i^!y;JR1_Rp!T#i&TdW|U'
    '+6sadW-Zj!Fup^$VA>Cq>8JY`e5@LSn839o(ry)WR0dufX3>hoLnY*Gam<J8hH@79yr2vqJu}vey|d&Y8WYehcErx!k2xi|7i?oTjd(!j5ld(DEg}ZsWeaFM'
    'N0_eB*g$CxOW)EW9yYM9o^c(fn9RRtxp15};u)1wy{R7<n;ii3-qhB~f+en8bcmd{-WoUUWchZyf~-S%15!TIm*xeypWb8xqIqxqu!Jhl$i%abi`q4}{2N`E'
    'H<n&sQ03bR7wrbDlxDkZnnXnrlz`Wc3X%ZRQw^3I5}kc%1lgjF&jiQ5EqodZdQhM41<w5&j{XcpsT`@H1VkIspMi0C+GK;FvmnSLF)yo#&xjdWFXmIsM(fH>'
    '_2O)xbth!TC8D;yj>J3O!A5!~f)>-;UYN#yd8O2c<c5d`w;mZWJx~GT%e{wEF|2&O=hu2Pd_>AtdAsKe4J=D`?nrLeC&M%xd?>$c@YlSJD=gC4y+M@b*X(|L'
    'q@T~X_g+KP4X#hrXdba4%_Yo8S2E<WpzjZdKSVbW^T*`s1LiQONxPQNA(S~08Y>b?AJYgkJmQ#!(&eN?&1aGhPgcO>N2)wj4fY3aBELiIlcP@(Vt?Qi6iAHU'
    'KPGUvzyfZtK&LQ+upq2ed8D|^@y1zl!8#X=owkReondnJ9Sk;b<nOu79&V78Z)ueIdDhiV_;`n=M<pdsSQ8^sHYcpzM>wu3UetlY`zPA@uGI~d?hFEu!-cRW'
    '{y*Q<8|v#;Aj;nTu+|Ssj$vL6w~r&-0U@IV4YD2YkH@>-smeH%h8%+2O%c}Qi0_6uUC1*Ez0&^cbOTpCk`5mfn^>+ke0X$7&oB>OLVTzPWW%gd4Fs8RDnrJH'
    '{6GXP;Z%@E_^qo1PgE-g<@~<<(J1#`XTW@WO@Br~I>0nw_K)isemW40Z3Jh*GN#dko#x1I;ggo-KNfB~lHfxxB)h)zdc&cy9&sIGZe)>Ljjx)nBg_BL!?Y=h'
    'Ew98-=41Tsp%nZNnEU203gh4pYQ7jf1Ku(P8y7VXIDO=N2y(16VW{Y#cMsX#WrKBjMoapkE3`g*q4iOiw_aE20gi+Xhfe4)$g9P|>u~a>lD94zXBNB2)*8Ct'
    'RRwL9ZQB6J8`$4%-{xFxICP3Kig4oEMg`B97XY<+fx!%=yy_mP<<3t+C1fiWHmozGB-OaM@M{C2d?CZ|g^Z_MJpN_KRy&AWHOZu+1pNycM#_z#;G$sXBL@%Z'
    'x4WVq=0mKb4Iqp(9lIAKEx8aIM}%xCAor@KzatllW&WrRz8xtLiiPS|PR_|Ff*SBhVf-_Ed>?`vLUwWjJgyXiaa?+gFEAK4jxq-uwa~;&tBgu_X|jG+jN)QD'
    'ed)GwXZcB_7P>gHBkKu11z9(;BNm>Jfu5YvyW@)~m1gEY2I&g?UK#6WgtExVNQcqR6h<cN+!8-CqA~WP#%E_(wD^OLCvG|84T>#uX_ZcsE6ZVOJlXQ9df@59'
    'w4n0J4aDIF;v-{f3XZQJOJ0|W5-;<m4*93(WNTPH8;4?k!XXzx3vvcGQVnpAP<EFmc|BxV%<}NZ%f$>%FJ>=xkCat=ce*nJMp6TY0pM_2Sg@u$Jx6QNd-O=|'
    '3N7)vdUVwWPH7vXNqoCLuK&77vvKFu8wlqKXJa|$-?a5K%;?V-=z~broL4(%{^eP$ibIi5NuQ0PpR%l?=@F{75+`)Tp3D-nEy|DcB9Ce}B>@yJkJIW}=0ao6'
    '%#?jI%MQT$5ZKNZa#m2JIzLfnR!FO$@DusXd1A@ITmV`iIi>h%bdo0V^CKrIlKLb?f5?Wnko|!@;LOP~Kl@nNp=_3PWH2B%t->=jRRxGr%z4QCcbM-h(kqWN'
    'YV6X~>U9=uuRXenJ);36w$7*j;4n=~UQubRI<%u++a1v1wndl=6ph*b7kqDC<d=4FL#45cuu>gJ`U5Qy!cA>4<w2@R4eRCf;lith9C}rHjrYaGi>%|~LHZax'
    '&6WdKP{t@Xu&=3XH?U69V0*vJ!)KO7f(${-I4^(koY`lQxw8QcEw5>e5yNbnsDRR>oC`u(&HGK%A0dbF%N86HVH#C3eVk@gM!5l*inXjhLB(1g<&|$L%B_uM'
    'H9mPb<wrWDz^xo)K-)kr-1WPpEO!0vV7hO;g3_H$c%0<%xq%MN{pAkW215fG*R-QR0hw0AVcLzrmtq%L6Jl^NS_jju_7#=pIK@I1Fxml)O?z?(C=H{xDO4hd'
    'cD{_i;!^KHx_AL>=idn_br&EK1xBP`fo5~+Rd$M37uOS17>iU^{R4CX6^Kzu9pu{3w2FuB5NRp}ITMimB6jPdo39kDxRiT9`H;eP9HMZ_GP1SE7@c$Gf>So5'
    '@;c2KZy*_cf6m!}0k#USEw48d4t}=*D=<zi$_VYmQ^A6IYr&vl6To?Qg#~RGjK(@-Mb50edPW1hxbbW-<%iSnYCxOR>bGyf7dt?Cd~;p}^T2?u3PktnqTIEx'
    'tR|Wsy1mi4z&Niu&dO|sY|XYRup?YnMN(0CHQ;`AKwfo{Lv|tSUc~`Q$WEYVtgE-*;MFIw8ghRo;%@ck_M6?4PG7$K*?2(JU<PyWpl`AmJ0PS2-OMcb1bvoK'
    '?|NBZIfnH~N_Ec1yQCX{r#WeQfmtF8>ny@@zVPhm^c^alra%xn$@8i>WaDz72Mu%Q2C5%U)YaSTxayNg4>^n=BRmFKdAOhviU8{BV~~;bki!V_S%w8)?}5P-'
    'Ji-+wu(|0RzKzY$g?0x=w(0)tuO-4?Ty2lcCfa%}I(5FM9Yp0<aCAD_4Y*#=*`_O&@P6yW=6Ay7*K9?FWwqadADx}KlJI6$p&bs*(fN_fIAG4Jw|6SkC#kGB'
    'mdQCJK!&cjeRcQv4hUF{oSX404QT9+Q)n_-iBUZG=mRkL5T=h#Y?*OKLeGuYBL44MoG0_=o5)a)y+5X)uF~B_Iy)S?_p+h2Z84KnWO--`b&-Ry5ZQ<5C*#Wu'
    '$tor81+KEXN;fls)CU<2Vrr*e91xQ*N4?D#lJPF$`Wqlj$C??GSL25Pe(dEtFPAtetF^04qwrR)t6&~@D>odPw=$!OvXq0An%YHeCVId{Nf4}G9F|X$xGSC0'
    's)?#VH-FU2#}EzL`*&WJ^UEm|aIq^G(4m{kv{g`~#2;i+PQs;@;I90fN8SO^X$@FmeqUY<5{ckmYrG8*-4a;PVp^iTLT$imrugNLx{-BhBJkT{eNvXsuOCH_'
    '4^UdqvPTReg7VO$))DC-wm0X!>w{2<I1UlVuh>myR2ichGZ~=}1k>PWL-&vMt}vViamHjt#OQm2Pp;2=XB@1AY!A(qt&r{E<nMb^@TR6PCL&LAl<i<?jQ09j'
    'F{jJ|L&)X!BR>e#LKiI_w{vTRx_)Cq0AoES^21<f>iR1ttAy`lW5Vrl)0;}3D-cv-jzTS8T@Cje6K*G3j}Yeze3Fy|57fy0JfQ<iDZiF}vkN>)`mlqgNu_W*'
    '7Pi?!Gb2*yQ;i`S$iLBDp)^S9uK;PN=%jH6-$6t`ALfrxZd1g=4X{8Jl2@(b?g}CG4V9mPo%uqAkX5`P6=NJ7sm9-aVb4n1hYE>@J^(*qXc~TrGsLo?$xSzW'
    'Lak~Ni=3kf-r~E-?B|<sZaL?NI+8pz6tz72Z^H^ucqOU|wu_lD6fwp*)`}e{jgs3n7bEG6Z3m&=<-yoev#QV;D-UPQt0HaEm9M2$j%HN?en!OKA(m=VGU9O7'
    'v4U*;`)`M3<Lh+F21}{eET?KEq2J&r+LqaUDE1&nowlV56MPn%?tcsha1ktCvVousLhb5eI$7xPba30X_dgks)nNy7u==g=P48~}YL4t35S^9-$nmEYCgt5='
    'TJl6YotSxa-sDxdz1(ekn>1JUwk4ZuF*^<W0NNOqFeo)j0GQdBIb%{<^M*UQfsf+M`2DRiguqM0AN>+k1hUgvTH75gCT)P_GB~?B$o7FtAlvq|4Rr>?p(Lcm'
    '9C`!<m(K*+At5zt`P9UyX{A8`vW}e#-V$G-DM5XLVw4LI@+v=&1zjN{=^+o4JD%19;mgze2aTWXAK0PNsJdJ4Mx~Q;Tjl$!fwxB~L3=qrlvkg)ta7E1T%jrX'
    'qcnmH<}t{qhwQ(Ee0>W+#^KbEMF+*TF1pY;ZUGn5w;Y<04LCPlrt0*!-x;EjU&pl(csL}DU%`;wCO}h&bJTG=P|8;`3}4M~I>QiOWQe@+!CCNzu27($eodRf'
    'JPmw+)BHp_Lzz8bj3!Z4vAy7>O{kPFtvKL!TZIQMKb>;E@A^@+BaK0K+Jl#p{RY6HkH*CiFUa<1R7#y`j@xzvQ7X*7j1sS+P&rzDEjp7T{k$5VNU!8aSv6(^'
    '88e?=)?(`RjnR2Vw**47oFEmHQQ~J=%YAz@Dm42VkBQzP1*7QwC0_t5wLA~LXK@34%h-GY<RgJ0=765i^9A2PlO-8$V9*ii+nHOpd0-g_C?Q9=<-=>p?`MZ0'
    'VQ0kp2yn92l9tDn&Edqd*u>nkxLuGgh1eLLM&BGDL-bytN~){j_5leyAR33umue>obY^TDdW@;`Ffv87NaU;ooWHB}Z4uUB3n7uO?cLhL7V>I58`VQ@@f8u!'
    'I~(1Wq5F|ZW`~K?_@yjBo<L~Edb4<1%ell5oXP6XQ-oVPP|7c0&^{3gFF2XoYi>6nbP;H}nR4hS$Wn9>Rwuwdkm1@eD{P_y@*_RK-$4$tdmGwYC80EtQXD~v'
    '{&smWt-tPCE5oz8od63Xz;>$-B<HqAX{su=iK~+M!AbAswP~Y_!ztzL<p@q1{CbCygIC6I>-<jf%!y@?_E5^acv{f{!ul&PUT|Nr!P1>N1hQ#~rG)r|*gW{%'
    'pWvsvR5K`*or!JBSm!G^o$81t28@Z6-E$f>c%7Ze@Ibc{0Qr@TZR96dOc!u#jWdD$l{-oB+&4R6EANDEXRh1<(YOU`Tx6VJ=5y3UXN7bCXxwsSSqq+)e=gYq'
    '9SY#iwg5sqK;p$lX)MIf1zyk_j^#JAn{|OVQ0nghgjb-@{GV>XCwgc!gu!y+#hcBdZX3`kJ_&Q~u}lf<gn^e&d)^^2I;jtC#qyB4a}Ga91it~TLv+URw0oRU'
    'DbIk$H)uByd*vCI*YL#Fff*tWa^p@c(>OraUpZ0dy5Z2hoH(eIifd&Dp4IS>wY7B{0sLgXJ!4WHuMNNs{3i+<@{{Hv>n4R=b4Bni=CPLO69O}h7Wq*c%t?|U'
    'R{du=pIumAU_S~BQX<A7;&qkooo7~`q^bcda)kcjSXTM&8EiWoilec7HVic*K3I<Xsf<G%k!$|)s!+6eZ9cIAQ3^bWzDSuwciwd7tU+W1i(F%%3Hw)2p-$=`'
    'U!<4SCv0|kl+k0B&lzw%+JQFmw81Y?bRcwKBVDQTTK<GG7D;G^N~^ZVw|afxS%HAcE~1>O%c~D~uLXuvidk+_D68QS#LCu${85lKydeqb2bR{l2qymu7H0Wa'
    '3Mi#wlg#z_IT1>DL(84N4GsbUU-^onk^-{Kcmm-S&8zr;=d$6@yrD2E)hR=Df}(6BNq)@_g?aN1>Z+YGY01j)eeX=Yx*F|sveXDM$Sh%Lm3r!a!yyVv9{&8%'
    'C>D+8RWw@6Gr1r5-~=A3c4kDBUPpv4obriMg$hLXKH_@eDS~!~hT{I<3`Af5)WMpQa>rwh!RZ!I)bOVL_)ZN+<po9lD9C28KVInaV}IkQ)gcZe#AKw|&H}q*'
    'q(F0R&jphTKCXe26y>k0=S(xkSf2`^9a9?6Go4r|s7Sd1D>C(#RXZ+=y2iR}Kooh-@>wTK-Ae<V_Ou(zX&<rt_H@+_R0PTCuQZ6YuF@HC$m}qr9S)<CI>eE#'
    '=jz8HW_$$`EmHaIkuTXo)F!xalprswPtdfLM}h7Q&AfS%s$_D%+h=D~96D7&mV=?ZY9B}>W_Dn;ZVXcS>nc2OcA`3wp9TOOYcCtb1P2pyTiH_u0xB^R2n3q6'
    '&N?B9{##A6u99*>aUL~fNXO;{HQ>vrST+<8m4Hb&Tk0xyt5h#E7&{!N27@|7&|1v?ki6&*_5_TE5N$Ay5pn8Gte8kCa;c@9wgEoprIf}pBltu&Xu(`n7*ydY'
    'wH>5lv$|?;wu1FRs6&h>loo8OXLB1Cw<N;u?>mC{tv;@z3d#*g)vMd;*?2&FSr<KQWQ02+qTgm`?@*oE^!2l3MqdHJ>BT+0$T3?V2nekKJ9mp;K`FlhlIr^E'
    'YPfq&PkoZpK*o}W?L*G1_5sLPlC)~b-I~L?8pl=I_*(1skRf%*nP_OzdD+1t#WD3UE4bN0n#yX%)nn<H0%JV+_4nL}n;j$_d_bY?EU(h!J(e0X;GvOejM)w`'
    '+tbuIy_^e+WlRHO(1MLqdGh*Hqf>eaLL^jI`LXhZ{7A342+ALgBkav^vzs!Yo0z`A?1aNKuYQ*8UCnT|0|F`?7e^x1ReyVkp`C%0KN@7fRC)r>y37-YUtFx='
    '@=vhaP@bu?2!nD1Br_-GRjawJwbA-S`)mR)Zlk^KB_$=FV#R8PJ2@Rujy@&Mm#=|w*^-^TY1ww5$-K}uK}6Q3bis#r*L~7^Xo^be`K;_a0(q6k_gnJ=?shmd'
    'r$4pank{CQ?#K^C<e~ma>}QiUCqEUHA`e2b+_u`8;C=N?#SVy3$vh{VB|X9J_=jtoZ+9?sYJw~Cy(H(RpYEot;tq&WgO=?%Vy`v+`f#X^S<q*6ayUfg3ia+5'
    'tQ`=gas)Z-<hJJdMpKPJ8Fo@Bg%kBT_L_rlu^UZI-cZgY(V1pB!|A<px6}3q(6lm`YR>TwSS@JVBf7sq(isSAL6gwuQ&;2oHC#Nt5CU<uz}CULvi9fH`36e!'
    'N^<gAkmL=-9;?RXRZTl2I@F^)Gizv)E-~1wPc-MrEG$Fr(t)+iPQlUkhYi^)Hh`L14=Qs(2F5$B#s`cyE3Y{7N8@3za05A|ct4|?8^ZYwh*9Y_nUQXjxqp0J'
    'eUM7E?J%3XuIjlcen)Ke$z<EI9kqz*6fwP0wG}ThFB_MLzx^fnAt>>qP>DjFfhBibgV*P11?~m@tZlWEJ8r6fVtAWB%fRzTy=}>N2k)hAdxY98#9okaV=mUV'
    'B(OvA52D|7wG%D$3&tkptqy+kV2UVsCRyGv#(a%GxB($8=zeYCKuNC7#oD%HV5Au+F$vezCqlCPB%_1e+^8NPJE5ER4#>^D?>S^imkD}d!Y#U-foOEuwbVAP'
    'oSnqDd+D+6TJ|$p%pdIb9mMW5FQ0gVfTOp8t&P{&>x|ARH{#{QpDX<_N{-AQsa<w3blL(au&CBmxIb6vN+6{P-d&rDZ6SyIM_X4IUZ)ojWxv%4JxEt`r&b_A'
    'E9NMdK)UrJn4bf!)eSWALGIR!xWC=Q&M2%-HJW?gPbr_NMxyR^1ku0}&TzEW&(Q<+#5mjl3^&l_Z@J-ZZ06k$5g#YCwuoJ?TA)ekq<;EDHJ<))CeB}cY^XG)'
    '1HQBv_2UA+=3Hr9?~u}$bq)rT2{@lF@oneRcneVu%e+uLdntQ&MWKqt{5rfYih$D;y>Qmtn7Y)1#mE5OziEtrz6s8tboSh@T|X>u;2SosiQJ_xeNHNwBaV~='
    '@r#pw3Ua~$YRCgAr@CHqT9}O6i@E%cXeW=Oj7ckTn+_b3;IHljnVa3g4TjDb<|M5InasiC=@X>0v9bP-?FIOc8SEkch9%;FKT|uO)BLeFHUy;AxO5sDzm1YJ'
    'Dx=dG0y(Q0+A(0anQ{VURx^Jz%78(lF;F{+dKv37$61O}CNQr?ZGFZ>5$}dWbHuM)-e_dZ;lRmx;~nLOM28xze4icKa!H|J%9ddZ)-$o4%nN2ntkV;Saw;XP'
    'PI5eNsRAmN1T4&Y=AqQYs?g)$4lT3=vv;Mg@`F%`*bNc;`zZ=53@zdyBhIX{ry7FjkQdEPrM$GO9i6}*l=$QNE`xB~=T*F)&t->1bI8ZiLnonzHHN-D&9=c%'
    'LUwZU`-28p7(5eT(GyI()e*O$VpI5?HV+=#p;e!Xt{>#~vs;o;Se<64mzYHzL4TUK$CnT2OmNXUi^ji8b$?I4ZLoAY)08>vx9~|gd6zlNFf<2d<(Ta{DP^@!'
    'v`~FPczF}*km58AeUAT`I?T7S%*$uJyaPONG6}V8HgyB!-$XcFUs_Qqcfew5rLOYb&TZ9CMK&dUR=(=Vt7vY&>ZveDC1x-JT^<!WQFHDa+F;4$U&}v}Z)(A0'
    'b#E#dG0ZHhw~x-LPXaY$BpGry(7XvUq7E|3xAb|na|m5+!3zwpM2w>|2fJN4jh~XHBQRJKnJC6M>8BJ-th6L}9K_MVuP#-esQRuzKnL22Nvpj200n*pVswf^'
    'MwkjkD-EVQ5FOsu<5GcW&PI^g!s}|hd63@@#y5~zhP-cHyRCe;0wERXY$7E&14tbK7l)M<iB8mm>hw>R)Vr)TsLRpQN5e=ta#Tk5>vM*oRE>BbE^Q&lL)6w&'
    '4SEG*)7&q%-eDv&ef3vS>C^;KY!`-&OXnHoUe0x0hj99=l^wpk%1<aaTBW~-Yp^Aqih0%FKWC~wOGUO(h(n!>nonOzev#Sz*_eoaCGo3|wb~6>@y<1I>va<+'
    'ZOVQ>?lJhuoPEJWO05~fH}-xG!mkm#p`N?bJUzV}d1m8nSnOd$;>oOi0cF(q#gGBl0B?p}`wPIjEEN|2Lj(*`Rl%jq0|iG=zLg*|5~fvm%1F4pmC+L$Mr#Q|'
    '^s_yd@IiMp-hdM;+w$reJ>fO)Ws}DqsR_z;VQJMKIyG;rSFb>HVs0qg8qj%FIbZ#E?z{3NuheZi&Pr`DA3Kq6d>@-B!4hWP$V6?=^X9^~dB&g3FBgqRaGW~m'
    '>OkHf-S8bIQseKsDZKHHntOY6c8Jg3#wt=H1NQfy>)V4&R3Z+-tc<z}<4ETU|8<8%r`RA}ALLcr<9hhG09n+T%k7zm$qSH*N^w+Hj3CM#>?tw)JahIAhf?YT'
    '?Z)+W^iOCv=(Rf!xg<eP`jo5JrXF@MK&2Sy1xGcnhSUCAdZi)_ht`4h4m{(G+gdTPQsP12DzB^kBWHJ`UB}WczwLJ8^z()G4wdE$7{q3I)lGM7t1xSWvmU4e'
    'WZTl634V7lh=CJbDR|>156uioi{8t|e4Fxd_r*d55_Fis2t{{X2x5)8vi|3bg$l%}&KztDcv{dwF_hj}<8ZMm>7RgeA$-LIN|{F?1yNUT?<VFBUFr$sK94YO'
    '%&P9@l7SrzjnADy$x=r16-nb{GcI@dXGh{j5Qqr!DxbD_%~zG|a46-?0;64C^@_<-6l?E@i!M$c_ZgB`N_=%6MTxtfOW1Z|>?_!ux3lin21+A-Y8hk>8=%2p'
    'uf0t)76L|e%dg7G|GQ13GP*+{e=4)6`e6jEXJ@!Q{j&pNR5AyN6Q{1;%|cWk1UfMT%f~&HxbODJCqKyP5r+|CFptPsMR3d;vnvRZLtb@VRlfAB++b)z1(GYL'
    'wpD;9JXccIPVvU58AXdhBIaGJV9{6tHYPfUVC-Cyeuqk9KDafHhaIp$33l<^qxvLJx!&CJX0@23#NJ(lN_xd$V*oON0T8#Ie|~k--zvS~lCRWnf#ofP9{=R!'
    '?<coe>mlT(ZnJ?5=KgWZ^+75Rc0iYzS6y!iS?i7<5-bjhkJcNH7r+OlJHl<|wUdZA9dSg9j$0cE(GZ1?O4uKiJcA;YZ8_P1JMEa~7dVK3-h%T!c$f|+zT7|<'
    'sXU@$0|u-^j_Z%>6)t_}ws6bOGg2UN!W6&B6wf-lX9EZ>H;@_|V~6QNNn<XzF;<@~{KyA(UQ&NLRhQm`1(Cd)k0A1cQ7LqgW;NE;+ZzM6CqdLn9Y`p-+ExJ{'
    '$X;a}RtY)EQO~w|H*<Y`lvAn(qCmHwS4p}Wys)-lheKnuZ9BK1gBfnW=eF%^g^_09@crupzT^Ev{rlH|>OhX;i>V8^&FZ6!R+&l8*srVlD1J`Pq|DN1P-DpM'
    'tfdvKsXxgVxn={P-GD!QKkWv5Pf-K|G83F_K$I3F%6^kuD{Fb2iLft<92TeBxYQQPGhNUXk=B|-*&%Udp2Qh}l3U<g>%$WJkg?O(uNjmo>Tz;2D;J(iJP#T?'
    'VVWwzhB!o;^|@WQdq;xrWXqUW@$Mn+{HBl97)DpTseTq~TfP63cdb`9#EdL-ZP(TCggdnXQG&K3-R{pgXmy({L*h+zkqpLp-A1d{ZS-=6c7<;qh|XNg3Nu1X'
    'W~A9<dYXO1jn4Kcjh6Ve_e4r6`x3U(?J3y}hZ=LAd!w$}iH-oZpf@0z8wcNx`DsDN9wlXyA7sPcL0o@(Bs-gu-9Ra&9)*rvE+l}Ro3tC;@C}F#G&ngB{@_~d'
    'c~|ZgBl!>l#IT5c{>mp~Qr!iv^qZnd$*cW1en)r`UGUW_M+nnL=Y~r81q3+`T30(?KuZ@7%g1pMzgZka1J_lkWOShXDt_3=<rm3cA<k?HqVDtk{l*mrDG^&a'
    '1)h02;St1Enf3PT?WYruqRf;Aw|lc24&6zFQLbC^?HH}wQWN<}D%UE=9NKx+_PV;8!_#uCf0u}J_UsH=0__c<Xvg#F$zWf6x%v2E!=c1HC{50*{(c8M=*y8q'
    'TX5VW_R<~ZmC4vpSe|r%!y&=Y08ZQzaDS^mze!g$5qyyQTk|TNPG-O4J|vX>R+XS3jC8p_9^-~`pV0aVL(Flc&3KIQ`koUpI_t}Ci*$w1qguPuOcJStI+Wtz'
    'XS6#bA(h$7q!*&t@j2AEP8im`let}3MC~3JF;;OX)dL52l%Y+F-}z2rRK{a*B}$Y`Uo4y<Cj(}AiDsN31J-zk$@#JsR8ZnaIf#iu0p9g$5?k(-5oAEwTm9gT'
    '9Utm^yjA!PGN961jb(U8-|*$t6N*fHZIeeF`$bzk&o>-xuryXMu6E9eQcuYqe>&#?(FL=pJNZ=h=r2&4gv#o^gS-KfHh>Zj)NTXD`P$9s!T5Oxe1oNl(_(3I'
    'mDDJG2c*As_&GJZ;ZW#J<>2k=P2smaPxk~0#<)f5%R6BGW$B)RPNP2=s^op8RXQ!Uo{uF83RD6nF~3{@pVr^`Meg20@X^HxID0WzK^dLuI59>ptKsY}8SUce'
    'hRB{=u*Ue=#7ae_+d=Bhkd+^-<k4K(e_U`V)jXY_MuzO90l-(5hF3mgg{zxLL|GtnuB)V-^ZIk8X@#M)is(mD?sp1aJQ{0zl8S#J(VoKSZOmhNZzlTL;n2K)'
    'FzRzCw4nBZhytKZM9Feq2_~e7BGVa~c<yK|{s}LhVhEmyryZ?=DtxnnzM??;vfg$X`zgtxrsNk=1nqmX+FO=TbPg~YTHIXd)%|?K548iO-hdSEC$c)|jqz&L'
    '2ApB`-T_25@d~Z+y5K(}l<H<PA*L|;b%#IAvoV<n$Oh|XL%aeiDDlfRsgS7=_*DMRS?8DF&M%r|U|Rmz^|oaEQVB)o^X!nlG%rAOyaY)3OK@>o|2IsE*eLT~'
    'ipSGNz%r~NTNK5F+I{b6y6m%p(!9Xfa%Qy~5c{om6G|<+0MRe*dRX+w&F36pB~n3EkKQN9b_-$x?_h(-M*>64!PqcKUQ;lU?iEP>0Lrhh`+}lw%IY+fGZT={'
    '$!GD!i_M7!kkWq`gVSE+R~yLP2Q0j{xd8q4YO{)iRBvk2SlwXG^ECPHcq2bZaYJVd@1f~h_ieSqc-gW?2B?3kPx)&uU<V3x8*oDLXI+gq-#ym{o#%Y*n?F(%'
    'x$m(8UQA%K!=aS2Kg<wmF^5wFKL0k_4!h<lW-n{TcbI=UW<<n-Vx;?U76AjD4sya5#mH!JiSF#;tn-}7u8$=({(kPl7UIl~r`5J4hZrdu<n(LQBB9n8AXUD|'
    'Zj79RFTm*z&BfPU;yo7^Z*k+&o%;q<UB8+Q*xnG$8;s#Mh~vnx<W+U@=tqxy_b82J$<dQ!S+qR))<?FU+!B`JB%jp(&)d}?Ij-{B9d#iI5bS?rZ44$3L0AdK'
    'J##bf75U1?Y9WMvjw&XVok5vfrLL|fOsx+B)y~Y<OzF(TGJ9xs9sdaks6c~6J62cSIQ&e1WDb&y7?9E3@d`ky9{l`W|2C$qeFc^pYR@zRn~KNnBH`FYbe?W`'
    '1^>o*HhzU5giTMYi@V6`lY&<4Q9enj41u}0FGuGB84AHiv?0vhJ`Yfelz=<v9A%lKkt#9n46j!pL5;bel5+?3-L+AV-kE6fUmL@`RY#-A&*iU5b2;kjMdm2!'
    '46w0o|Kkl$dtcaX$(EEJr54q?8dp~w)h8LH%)L;<aI9W+dL$|&=g~xPhS1Hsiy4zs!-t{7g*N_^b+=^P@nd0%g}gpgC46eT_c|N)qT?xbtwtYUyDxda8A<0R'
    'L%S80RoQuJ!xPObDCYn0A8dQ}Aco~&=Zz+{A(i3pzzf{_r&T*{RrJ<vmp#KKA_QQbDiJt67PP;CK6$e%JdvGw&vy^C>`8JAItnSh4B99%!TWG7Yh%hPrJn6+'
    'yO~j;PHi%?It^ani`hW<t50^|&}qsjx{(=0pE#bVjRTFuJhJb3HC(!pgpplfARu>)ke_fP=^G1=5|{nHO5J&GZR*srb67qd@eRKKRI%B+oyQX1I6vQBz?N$4'
    'i}K#>)?~<owVQP(ij&p7CsZ1(&-|y-A-nT*8os1$u}o=>fJL^OfuE~dtDvm%HxL9qj=CCFcU_)vXdL04DHe?=b1;`JfHwrLcN7qHLKX*Q<W<}2KWg0=FyN!z'
    '{9xR6D$KhvwoJ`Xzd#VR1kB|=y(J~{@w**riC9AF3Y{o~)k*1!!zsn=hKenc5ZxM{&a%;JwwDim+t$AIDYq=aX1$GPj^6VW=T;Sy9{ot({1p$!guK08-Ykbq'
    'GqWwOE`O~!PzM=}oPf4%07tk20Wx^@WX$GjS)La;8f7~AgD$wmPjeJGYQU)U3iGb>qEm(Ta^U4Xxqt(R&X2;OACh_KE&}NUoBMS9WyiO?!WV7{205R;zru2w'
    'tqMbVtT(g~4Pr8iu{#z$Vss`z^sX~O@@qYhYV;yW*aiBNVd0355-AQ5^a@|U46Rr#Us(OFBz?D6&|VwHU=4Eq2^i*;hqOMSD-FDT@n`sE^_-~_4volng7A^Z'
    '-4nA)DiE!l2TqD>=LW7aE1ASSDiO1sKX+xE?&XQk+m5v4*li!cH{fRt`R)GI6jzqD1W;9t>ud@LQ6Os_O<p!LB&EJI!)#mhv1e93f<iCuVT02lp^V8W;fK){'
    '-CPR9%e%4-A@b%b1|(1Zj{OQjy<Z{9{%)2(j3X+WW1cESxAVvu#F{jk2(KJG^_rxMYw1MV-+*}=cDIHK_k=@d*hjh1yvolQc6D+ze^sg-*;hL*?}`I7;~<@!'
    'gB)@80$=F;LH>+GY0Z;A?1qr-r8Rfnq!tlQ17Qa21zKy4K^dPNqj!(+_<e`i%flq=s=Ji84GvN(S^&h#!~V0ty>ghOJ2=BxhHszX(y5Q~I?O37DyujIsRi(M'
    '%<xYCb%7a-VEytcE*~C!0-`a0zDxlVLA&0aXF+J?0HOqqauJ1Z2Y@cK*%?PrDhCS$$@^-iL48KftRhIxu)JJc?vn*YTyLs<TU9j@GZ{I0pI3SP)zY`~&Sb>g'
    'Mn&EbbK7;iDD{`0W0ine-hBHinkP(i1`^bm#mpI(obYl*&M=%3aeU+VYY>BZ8-=}!Y4C(ZW0-lE7G|NJsktd`-e@2T1}LD7udd&piw9Vr7UuoHlXoNdFV658'
    'MQOTvhKL6?nhUL)ay$N<qRc30E#P|%-Zy}Gq!Va*C7UaBg4H^Kg9{6{Q%jZ=3UE6#YzTRHsv!3^-t7lBw}WM5!~;U<y81-mKuamDg#3hFd<7ZsEjZUe&aOU<'
    '6MDpTsOfh94)ad*fl6~G=I^2?^rtQ>2WFkn>FxDe$n=&bTX$JGfN1jVp)GD{LGxwl=v{JVWg*5+_$@{`LWxU1;7Vs8R)={MmkvS)i74ht-p&fh=&*~tb?!x#'
    'Janp4X)D!<Ie`6%gkL$>LCa#7F$URr%By~P=}uG<>Ey4*p~$1+Mqn}8z`*GfDe`pk=s=|u`Q;n<<r8Lq0S!Ua6EMAY(q>$%C5j%=TW}^a&&_nHpjf9iO|<kA'
    '|Mj?8i%i>j9}&PKha2#x$AS_+59Qwtl6=TNY`~qh;A3sqnZwU&`6I|Bf_c@gzw_h=86{$r8#D82eDYk<@{!lCvW(sKCe1GJ13rW}#5^cQkD7^p^p{vuB0Dm{'
    'TXfTm7#WjO!$%OUdNbr}VSMf5*E=-pqugl}3Iy*C_C$T^G`8cS&wvwuwuZqLA^@d9HHNqU9l2ZgXjJ;@yKs$^-OH=AdYtkJ2dUJh$#=BUhhF!^-J!OOLSs}$'
    '#QKN2=+ig9pOk89{rpvFo=0Ad(iF}USEn%%I5mw#^c?2MQcsu^I+B^^2O>vQkzfS!(Rqj4YuIT+UO*}3pE`^LLteP!9~hH%LSl4M_u`44q^_#;_LPdexAW6E'
    'vNT!gT`+k~MO{W{1a_vfsva(H`-D<Tp*N*{W6*NuA+2`%xo<Xy1qJDJJ4K@hdy^mGM=4KIZAaKi$%Ht5T8+;X8x<fyiFq%9A_?<55!ouL%Zfvz%ey2c_dAU<'
    '=oTB@t;-pO(+L?s-h7}bLB9oXLP1KvGz1)bChq>t2Q8=E5V9NsPpjdJOU>No)5gGyAmz=6-A3$|j@WS6+$Um86MSisr?6tuZN!}8?E4>}BjE}zpb61AM2h5Q'
    '3!p0~jVm%3GIFdwk)KkJ6>(+q$-wxL`GTE&!j_;@8$Xl@pfg}&))PQ~jy4(S1(trdzK~HUjQ^m<4IV>C!$jB_=Vsfq%k&aWWnKQN#F-?%KvTBO@%7_PoK1Zf'
    'P^iRj<pU@WGceb`fCzw)tRi}6>>a-Vt2~ZyR4Mp5jX@e;<hcGvG~KyXP=&i*AUtuNeur76l1bwjx;f?B`4ln^P;S&__+1OxT~2YOzi5I<&2bLnc7)Om8x@>d'
    '<b#T~yo#6HtrUEuG%|<$_G2d7D+?y&Hcdx-5aDCcaG)D+VV~q0Bl^fU=ubk$$`^=loPEs;&~@AS^qj&I42{!;yN@EPd^1&Kkq47eT<f^shZ<O+weMKHK1}Pp'
    '-1LcYc4Y1e)@7u#g6#SO{ut-6y2P-;K&8;VFdo8p29PYyY1T)Q8v9t<KzbUsAK6*!qZH)l^}7Rs^HVCKGTsL!L>Y0oO!1^Z@?PSR>OGR9HF<beRG@r;FnR%i'
    '9$lYkygx<$jiQ7ETBj&LZ+53eO>Z%cH6g<j4$xs9+COk#6y8rM2j~qTqR_ZsSDjWley+Vb0Wqp`CRcOV0q8I2O=dx}@^EYteV?H9KGd(7*ZL%-=|d+>Nvx~%'
    '3}pKjXBa~EL&*NJmeBs8K?)|i^vSQA&XncbWThGGn7_-(bCoO~a*say?%MDZ5YU3gf!t$nBI^=WVa=tu2kH&76g;`L)Tg3Q={B~0Z421;j+dJQ556-RXb5?a'
    ';Px$bUiHhf*e4(wOO0*LtpN?@5g#?fusSgV2q~Upbv-T235UjFpqxrOR<GI|D2<xH5KY`{wKp%m{rP(sD)VdpHL+>%BKmgipY6zpI7fhU^P;pV@qG^Tx&!6_'
    'qT7@hg_-ewpOXNW45ca8u-(0a4+O|A1ir(rU{p<wKR}8HPM$yomm2Tgi)9T0->32!p6Vp}n3mOf6MW#br#1m~0l8nPnO8LwL+22(vvDRkSRr{OdJ@Rg2k7r+'
    'zBvI=>bn(M9O~+NQsMe2qw>CK7{esN$!hc@IDlv@`|T5$?DUPE^dU_u=_yfF;#)E2y2~-Od9tC@!OCYS?aXboJP%lb1f8H83ibvwaIY?Q$)0QmN{$81vU=N='
    '>volIOG++ye@p?A4L3q>5JsCkJ2l#|r|BzD2BkFJIUFVJ$4tM!-Z!T<EH=~<g3)n9@K%sDPA%Ap)WQKo1A13p@p^29yXM_~xNQlcWzEP#Tx7sGm`vjqMka8U'
    'Jf;GmSb1w_>~@Bwi_+WHT(+;^zTmH)a0D|BR!Uu(8gQ&$@$qeTFT@BgdcS#u&ZC>ZNZ6Lccqn!BBdKHP1>5e|^oAi<mbu%#M#(4HS0U=_2POis2NI2#GvT@<'
    'X19lD@Cm2ngMxk}X+ZgW&#QiI*+YE}>H)J1n56XTox1!Xm?7XG6!X;8_<+fCtzR^Pj3i&z_f@#)mA8My76KXL9XZBV3H8)w+*}f6R>YNF9FpFFdxz!yZq$JT'
    'w3rcOxxY^wU8pKea%Q9|1aj;<ulgrEzx*hx(hc7RQr<xCM9#*oztntCnag7K<?!eqh<Q~_Ry*T%`1TYViWyj_SFfx1K>o?M0r!!dH`|6ruo`(4o~RT!fPhM!'
    'GprG48j~p5KhK-7wn)($u#RWDZ%?q4ufVby*?EnPU)}Ojaag6QuxvzFsL#0DbNUe65O5TD!j9Dk1`|~vMu(Y<d`Hh5$+(tXtD*osV3q;9FNTEt?4Eih!Nc)Y'
    'l(izQK9QQqPtq#|(nu+;f(W>|r>6dO)!<FPfa(sz>GmMXJd-lhB^MR2b+{RA2!Ro&@n=v*iQmfCa~5U|H(Ut;FcHXv?a1*F+*Gaq36sV#*}p56*6pjPkpHGO'
    'AreCj0A%m@1z2UF9k;RBw-*R|uerTs^=1<pQ-a%|hu8d*9;>(N7l5eDgpA>HJDzcwWB489frG-kgJPB+M67pDxUCcWl(D|R?)N)Revry4hEkIuDYe1Nk?Zt_'
    '0h}X>!8ne<ZqmI9t%q>|U#w`9;NS1UX2?+q*}at`w~$@0(_inxs;>$M5Y1H~v`wBZ=&l>vp8nPWVQOFXxEaA|+nqtNPUsEmb8EZtvIqAjJpojD;X&p`+E>Y3'
    'X3E@lUYfX_mnQw|56bVKJ={>Jk+kUh1C{nw43`BCo3|1JG35^qan@ToFlj2;GM%z~*k=l*2kHhwJ52Bj^chwMw11*zFtij?W6)8Kdx!6)Yfo8|q?4pmZgIeK'
    '-I|wlj^yoj0}8a6L`OE#4Yox$;<*R0eF1n%=IYMdC=ZCrt77D*6Z8Jr-;T-wQ56!)fZb(2CzZtb*PnQdF`tll)R!~HeLl5Rft<<DotE)?Dg|{Q(HQQG3x}v_'
    ';?D35=%77qGQl&6x`J?NP;PNg@{>@f9iyJayo!%Bti~^z662m29H8b0Mj)SXC@?1)Q!7T^3VE<uZ>$RrjdB!Fb_kiDf$VhE^5v^x1R2a@kXZ?NH;Y|muB&7g'
    'D|3C4QkhvSrz?!T{y{E>0tj^S2Dv%7tS&FxDbG>rbTc}q0gT}?IzpQZvR%NUV}yuq`)~G;sa&<X7XZ;*msS6WhG!f)4bNS~BIYg=^MZr1B2l9DVjIXOW$-Fi'
    'gz_Y%a^yOfK4jQEb-tWO4(tNvPwdP@`*v?Og*h^M*khAcMo)@6BoJV*5&ZVQ0%Py9iWwcLUqlKGmesgC=3O4dO2j0_b_&t4e?Zr@+{_xteCY$GF(XWD3>g;y'
    'Al@MHFW~b~N5W<J_!j31+S!gn$Wa({QCC4f=)(MX@*FS}x5!UW9mro?$$kv8eoAf#xoN;^Ag6MB7<X6DU)U5IZTXx>4;_C{LXX1}s9&=CK~3s88bC__hcP&t'
    'U@fRrhY;=zTSDx$tcL5hEf*r^Pn>Vt%8yl54i<zc<Nt;oWTLvN00AXtmUk(BdjtB}4afG-xPS~!*riyqe^=9IEpPf!{zl^I2wR1v*{UD9(7Zyw`r-bP=>kN>'
    '!&f9bQ1AFfKIiu{==cTeASarBFG7ay47X3VW*ZIL^w)QS-rrjkAhkst^vdN`;0Lnv1qf?}Zr?bdTII%Gvb7O<^wtv@A54I4ukv*2vcl5sPn6R;fbArAAyi?u'
    '3z6dx?dIqr-e4k8ne8bS1b?tE_ueV6ttb{C^TA@;J&5+z#GtggSgFD=I>fO1TO=VyX-LK?r53HT+ZnVOmk4OlPPPr4U|kl{<nKydp?Q@rt2plwI;h%0^H+lq'
    'u{~C=x-<5xr;D%|7z_5aGbi8&P0)7#DoVxdd6nKq#PzE-3D0~!of)*>xd9cH;!Hh2_CrN=l~>!A`Xo|Hodk1UR~NVWpMJYKz*eY2Iab%(?*lT@O^`bU?Kc<n'
    'ehryN){jgt{o00XIXDK3`zODuO@Ct#uBceK7inlt`c-*1-`?tzR2C5TI9)f!Qde0!8nUUJ`RbfJfL!{6$9MbmcX`!6K}&T4LORe<wt6UZ2;5q)&U#A9`qelB'
    'Y!=U7s3?LCFj#@7=U4?;d?P-=&}nk^_(aJ5jrHtm0swb?4jpE%V(Hnxiy#zH<<<AnpZ+wr=m|Ch?W~ExBHFpG{u3(QRupH+AFGPc?0K3C0!<&4s2nVxziNjs'
    '@ILUPr#A4nx!lnP>&Tmcud!qJyAGjM&Bi^xJ1~aGm+9GCJgSVts%1WcY<uDG+b?I|A(lqt4ure{v1&4I{_5a5DUDfLRRB~0sXVC<_SZM;nc&mR)gY@>`h(eZ'
    '1Ub#K2qNUX+FEkGzpD15@}qQ4Hnk(fThLx=O1ty+m{Dk@Jc#O0sqS6wmQac6C}r%2iqRdaegEw-<6xzf`9jKgk?rF@W08SeV%~^$GVxb2>EQ#6B!0!rf`8<|'
    'M;pPRqj=PC%!!{h^;U;~MaxZX0Dsk4lVN}wv3Q<So4sGC<GMUT{pvu+u@~!G1zD-rNW*Q}oppbINQBxz=n%3M+J^J0|AO;tN|mjqYdC+E1!nzyHAkh$EI(n8'
    'RZRK^DA6~iwuesW89wrPK3L+({-^p_)QLO^m4s{)mDV*{OiOhFqRB5=nHilA-dlT_t#K5rhy4?gXVu&)dC?DmVodvkJb3K+{Z6EHfY#6+@{5tXA1=wLLpQ>D'
    '3YVA|@Q7*?k((?bIKk4yORU_NajdRykgqsErz`NDLTexcz76OSk_l+NV@K&<5A(T%1C&nY+2s>ofO|3SyL-$}Ky;u*-n+c&-r}=Mcc_x;jt?4SNxfa$t|pY1'
    'oPRvspzG+>x{iYj=DrHcNlIByCtX1WS>*6Q4`9Wig6v9(v;-OO8Bf-=h1?^^;NG@w3)#)tZS`(EfM|B(_T4BuDS6c`&y(fnpiat7zMuizufX_%*I0|VHyvVt'
    '*bfoo10qHh*=|pWpMU|L08GjRjT>HjUE9aDC`jq_m)<PHFqCk7z#!)*X(3CTho@CqJel(JD9ydsl_Z%cXZz-qUnP8hPS)ioIn@p3z&sE%;MKg1d`m|#4m~m7'
    'C3UZtI`By9PieQ+5UlK9B_pSkoDd*c;ck#orjuOZ7_v@=9JCp*iDHHW2dOlpLG_6l9w@o}_<hP!_AQ&>8)7fapuAs~GmfByjIxk3>Ha0V>nXWm$@@F-qh93x'
    'Ej`)d_r3YBx5rlx7#h9K-yLWP_x9rLdhOdT!J&|YTs-ObK~y~E%*VfIUcpH)JYSaROZ&0rob+-VSzTSsLe`VxNaVQ(N78E$X>D%ypbj89%?3Bp6YQQcw_mq1'
    '3{Z;MjrJn=2RNE44&`P9KT_p%Ip${|d$ls>q$Ejt(Vf!RPKARMY4<Kf7ahYLnw?RB%6_D~&p@;JvVsWz!m0ipU&N6=8<Xa0J(T}YWzaw4W9KJnRfgYsvpNC~'
    '>+_}`b4$iLGCX52JP{qs&(X>_j(k6iZ!@A6$~bDPpL|KF_93${K>B_eH_arnj9DfD<gZ7Z%oIGak_E7$l8W_t&ncSKLv>F$G@FmxAb*1yx{C3J+5SObawd=?'
    'L~}5v{qBcwX>3)A90uph$FPf4cY+K9l!(J<CqjH;CuS5%t==Gsp|ZTn+TnW>A&>)yQmY4zN&J*U{qfrzNg177Jj87}cYT?7284R-(s`JNQp79feu`|_lQlkC'
    'Fh^y=&v}B3iFNP^Mmv(=a>g3{5m;lROQcNRMiVAm#JEf|SHKT#mtiY?+q;V0A5{(AA;uD9dja{%YSW*mH1!p|Cjz!%)`Wiu>H_}R!)xKchgXCV>GAXf*3XyC'
    '9k4VJhYZpRMm^7VGGn8@9sAaS2aIMr_Pz0wTtO#PM)wBsZGdX~OXybN-hT4u2U(rk0LXc=y!r^@^dP51Jm?P#d;HR@KSo3Ag^m#8pfYh^)i<Whv{4^Gbel0v'
    ';xIy(C$C-<f!e<`SJjhCaUKXR+)!_Jz|zEd_7NHoMSHTXb{Lr!l;s>iG$thO22(nQVyPXz2(KX-M<KP#I_J@adDY3VjOtz=Wfu4P3J=_WqLRctf0f_%V2hZ0'
    'gNE)U?L5KI=?S1*CKrV=x#o^+ARgrcn}0~NHZisXzD5!Ue7mB3!tZbhH2&@8-oAba=Zm!uRLWn#z{TBH!9Ea&%{Yuw=<n1FFUVmxt@bU1zKDPE1Vkxk2{a!w'
    'A@G3!)|Z$E@szyk*ZU|@A5e)q6_$(`jns92X9Os=JYiRWg&1LG-M?yKPsLI$(V=+`7z2Ahec${L)bIsR_G4oSUsyMM0fT_-Qx45qHst4hl?=-&4|<d%T4`(I'
    'BN44wk{j1TPnhV9-Ks_0t;iL*c7E+psW=p#c=;~NQnr_q;L}mbWHH9=avXy9?lUHL+Nl~vDX_d!>T*9^G2wjm2G)dVHt^`8iF0=5Q)ol%eyHQ|vSHl*fu~4z'
    'y9*(vf8C_U7f?!#2O8mTC@2>>;{}EiF&Z%>kyfwk{8Jc+gWCy%IPeYfPLP}uJqh!P>S|og=2f0UO2GVf(lmf`-R33DRGLdf+g&1tG$V1=+p4H^1`=6}8<tf!'
    '?m@iCsP|@+_c23BKy<?^Wsb^cQT^>+ZN;I8r<7SE!i^`*Y;e7b*w*C&FrFcj=gp9t=?1=nN^}3oLosmt0_y`41%?taN+Z9+&cN#~8PqoAK*zxX;r4+%Zw)gj'
    'R^pFh2NZx%sxDqDe)m?Obya1|*v=k$!;C`r{@oTlqtF6od0*llOL-vKrXFT-eD&PS(#n{7*UkIsP%99P&Bk^_QVTjrJq$N1bAFUk&>*w#WZf{hx<Rg>uu8xv'
    '7SEJbH@+6*UnD+pg~#bzae1z)JiQFMF!CGF7xQptL{7Rk)$~<2GI-!5lE~XPV|tX+N;!%(VtKXGEO+;ekP0LyLE~^YB0fO%tDdNlzbYLqyZ_OT@U&Gl4w1{B'
    'U~yWpQFY23`1<0`tL}2n1O55a*a3josEVJwzY5AK@sn^hJ646X)SZdr6OhZ>SpM$&O!^k|H-*TPpu^k6?-I0^H6<*+wVx2HRHNZBZ!ZQ9^i$RckrJ^NdV@n5'
    'x^h)fpKgz|U19PkOho}JBCiC`!aU(nCZTkcx=B|$IGGakb}ry>LS<AME>@q`)zvAf`XHtC%Ofo$LxNTscz3Emv4)v~i4KXQ$Jv3-Cs;bOf=-|~%&Y#PU5ZXa'
    'ad==dW5k!!%E2>%gUts|V+LPkkdp<#RiH|HLojk(^6Kh|8}(6&(wVSNVUWsx>#Do%;AY115xtwMFsP43jnGk^L!DRM<(w+~v(^|NY}|Cnu)PDmYAAL7D!&!T'
    'weYVa$(xo3dT}>$;K?h-2^Q!yXpf!rt>zv%5Jnc<7z{t*(3r{ny9lz@geRhe6^K!)N*M0cM}k51QK-ipMwk(v!>rwsQ9h9JDm~VNQpH4S>?0kCy;#=GNuK&7'
    'RGvU!<zq9i+VPDa{{mTqHf#=;cWhw(49(9q`Zo~$Ml5M6bus6Jt9T&M*+7W$4HSj$uqAsFqVf_t2xrw*eg?BHqIG<AaMqxPBPY!+g%H>|gN6_OlPMo1p;8?@'
    'w0mRHJ(}9e5V1Md9(_I`!0fFqWAaM)DD)#AtDS!2drPfPFf><ZYI8&_<ZHf}%_MpQ6pzn1H-4sq0;R~K94n&It$Y={ARnW1;Cyt(!+h|9%HgOn=_D7sA8(*%'
    'S(@0Uq%3f+UyU+k>@~A32!m2Pk$Eg<FFPzWIE3DMAR(l|&VI9vhN7R1UXYTTCLCi)yo?_%n;<unVhF8gmp*O0BV?3eIpwb)&-Eke6~blI>n87o+6jYW)In(Z'
    '8K8di8tE@W3;?v9s4m4xJ9R2&zfq+*2^DMQ9t0lvy6UxF<If$lCm^IcnJ~&1IxBQq-5%7<Fj%)kGXbM?{L|!g)-S}|V}k69*|yAyt(fVpICM>cO~s+Ig!|+S'
    'Gtdkx`7C!*)l?v?#O#C->vh#VaLVT=ks7jio3P0lzoMMOKO%2!aGUWX6GOZQzwR<&<(YYTwHtX`7+tY#LHY!CO+pc@Mc%w}AN_XbM4rEke)k5_FuJ<>Ku*po'
    '$ItCwP5n($a^IHP_lt9$gWK*vNXBeeX!10D&L5~qWy?kKztbB$VPtfSfAy<w2-z>M&pJs*<4@(mFh&M#znXn8vmcw5i=m+X=$uD?NxE}G6^K<5D~4l$i*(6!'
    'n)0lBPWcQZe1n-J%+M>`yFVy?;Lu!eT-%u6Vh$3S)SZgqy0yxZkfYEuSXbi%9&3J**FgsR#wOoD261A;odumIAUeeWFNfAXrjx9w^{psKC*{T(wt&Uie)no}'
    'GX-S`*vfekx6_T{`Vtz~PNg3rj&iWlwREEk_Z~)Nu+qtFbK#gkuzW^u#i6orUhXN*tL`$8kje?21#(1ztTmP<3ktomtvDQn{R8;^izXxc?$doE@%$t$WR~^X'
    '?`ed1-ToJt(Rl*)5z$X;xmV7rJiaO#>&%Uh&&ZsW=6W!!Aw$N?_yYZT%Md*5pSY>|tS$6yAJhmxypKQn6uI$QXYc3fnkOKdE8<SOZ{)oRQ7VDBTTdWr*iP$L'
    '-4JrTq6-a#xM_)ZU<`r33BT3hM<-YL`0w13DadG*#XY=RPRTLZS3AX2_n(>E5>$*J=S*6EH*)c1)w!i#bMFTX&0gF&q5HG0>MD<O*SC~29yoNE8}fw)Guqx1'
    '#rqvVZbMxo%@}#vAGFI0m09g(e}MXgjOH0BJEa41@IQl-r5u;{ECz{)TGVD9?ZAeoW8n_WICML3bf3xO-}dr2WHQlP>)9A|FZR-(4;R+oZbE6elbHGxoqwC('
    'cg$2z{{}lbWaorRqc)j{7sX#epKD`bW+}A%BqQmI4bhn=5iZ#Y+@D4Eu-$RIGutl#Cqq1;v_|XDvoNo<u7bAm$aNy`$_;zq&}j>rx4OSp;^xg^SXUNs03jV{'
    '7^yhY<%GyoEoH%$4@Z8wy*PtXsyPlyz4PkBTNJIbwfh5IgFRVV^E`H?aZhDQ1G$H{n`+P5+FY#o`jZGC=dZ~5!MYmz{9PQrLU`c5_94ol)T1!GDQk@Rf;WD`'
    '#mzWWiW1A!u5Za>Ne5j}gr+G76fkmNq?&F2nGEFwOJmq$88FqCdfaLML-+o6s-317f~82?i@5OF&-fkv@Bzw1m8Qpb?o;~$C`Bgj%<%MysRgA0E7!p0)$o9x'
    '#}aYVK_0M->@3C|cfD;oHqKfE_JPgjdEIB(W7m%nUL@BaOB*(=@<ev+3k`1yp@$ldR*o^p_D0^nYZxN%v^l$E9J8G-+lXzLWGHp_b_Z==Kn!c$IbQb!M3Fg8'
    '6s{n8>17oc&$>RrP$)|Nt`t73<jLxxYd_oX3WRi^Q8b4IG(AuvQ-EmfVWubU4~#ihuLji4Pq#baP|Def$+fg9_MI>68!f0*O-ALrPJRRKO}x4N;QW;2NKruM'
    'xG1aZizCW|j1Do9=nsC<7O!!q3W;>o0VWV*wq-S}o-}#Fp)u6Z_6fXUhTVO?<XflUV;YUH-5ItYGg%u^Flo;70?-WM(*qlEf09m1VALX7Rxk6f95xTi=1+33'
    'y69hT$K|<7nKu`V<^^QZ%5#rv43-%jlQ*f*W46h)pz_KWuwuP2NKDCh8<rgfC{-qPQ-TOMt;Y4XJ3q+i5ev10fF*LeJm^z!XsQPjQ?8f2KWq)nDj{bWPKUS|'
    'fzt{ZdX}UK4qD`xCSOMrtusfJ)r2X`s6hDwg9Gzewaa<8>Z3F#ChqBCltKHqyV|<0+loYonn1`?#hq$T%u{uIlfIc&DR(oBc%&O+z?+MJbgA6TY}go2n}gD!'
    'H~U_<b@(en^CkXUtEZ3WC=QwYRVyU#>*{jv^@QW{CYis>LTab3+KIvr-BzrRQqU&V?*>`Wd&MU$4d<L-DCHakrh)ik#2p%H`$x!VTnI=GgPET2dV&Q``wAed'
    'O6YYAH}-_!lpYB<XHQNqpxd57=y<NH{t>MSX~!uQ{XKovo;l_+y|9Tq*pSDLx=rYJNc?@S%Shs2g+kL~6_y8i@{>qYXc~_xFHtlt*d2J?j77KLG1Y*_^e<0N'
    '@H~Oi85#&e&ryB>yzJ}WRE9Sz;~m<7$R6kVZ!4&v`vqi#;x7PuMeKLy7fwKQLKhpoVqs6dt_b(rCBj-Vi90#qWCdIWr4_mx*_o#PV9jN-;!xTal$tf_s(l8s'
    'sy}1@YQuukK;Bl-JOY`FCeRaiGD|O%p2WT3)b|vlZA0=FgJZ~_ajposf&w*ulzf)Fip%d1`8h@h7)a<XsjJJ$*Xon35^@|tM!sZBaDUs~60AcE2rf?5Z&pM~'
    '@Zu=ucra9&#jGF|1}YItJfL~i^%@@Tw<1~^(E8OVLxwl+X5E>NrJ7jdd!yX)P*+)B+578t0ZYe&8uCb)yM*vdRKw}DIlNugTgX8&9;Z|YFow9DEFL}h4yV)C'
    '4)=jdb1f!$ioj`gsNCD#U~{&CDFPW=M;8CCG}IQ4jPX06=I&TsPHR_TutsNg3R3Y<r?z;1nVM%DMi03=(k8Fkw*lqjIQ>+_hq`xRNi`shR2$+0jiMC?>I990'
    'AXQ$CPiQl0!%_#EC&`FAY@Rn$^qg?$Y+fVBH_WzW;}v^$FqIkGz-*3DZ$JBdAEc<V#1EFl{$|W{jeTCV)0t!6rd}QmYZFoXK6hhZ{<f_Z`+Nf-oqe-I^&!eJ'
    'XeV%~$3lMyynbtD7+$9+Ft{ApmY;f={mVE|4|%XNuV)%<*W3Wkn>1P{1fV%i>556YH8GAP-XCGz1k%VPx|95A!13hmJ!4WzpUee(J>TJsz)7nwZs@%_VFuRy'
    'D(;kHuku$(PP6W-R|UK97r?)MFxH|C{MCFPs5If?j1lEG*zak>0toyooSrs1$MzWX`ZE{y36=5<#&JQuBx$C<G@K!NVlvLh2zO1ctMEXnO;L6j_tnm+bThL-'
    '#bI=ifdnG*crfZsR-z~&CmehW<m=gtX*jSY3i*@KPM=HP;b%G9-~{^Pn|T@PlXTG_C-+CyRlag_!CCeY&Gw0gUd3(vyPZ@vOP#)J<u^D7X@q7e1mx7TyIIMA'
    'FDv7Ic6tw3IzuTk^TrO*;VO^7DS3GhOy^)k2k~ch-hzst0|@AJo#h;a+b_}XvLqZLq^aYyDT^_1gHMC3OhdSqZ>7Z_d$Ta#gpPla{ifp(ys_Q`-|`z^6*+4x'
    'RGa-laItVrcKYm_X)u^-3=Qv}-uT%H&l4=oUiHIUb>^E?lZ^dL4sZgZk-L{?PsD?olhzv;ubez_U=q{d{=0zlL{(76;nk1<zJz@LEh#_AC?R{<W7${n^6cD>'
    '%U6_(;{4H>QR(~_Kg`|xmg`_yD%|U{_rBEY^^Kt4JNLYH4E9u*|3IR%dtl^@^3Hn-yu3&vqp&(LXZl;oP=A?C`E3-&35U*3gOzX0ey3fyB=jY=WWckF%sx6l'
    'ee3k?0NSC@t?Z)sLK&~t?HQq4ty8j#bzrxf^wx?#wfY>T0ZElB@@d|5v8@9f)rUD6`I6~NC)2R!0sevu+nt`tMoK=cn-0busEo?`<?mYg4LU7UOT4}egtVXs'
    'r+c3?Z-oz~YgIcz9>}?KnO|_P^ew@iLe3&+-y;#dy$I+_$WG|O%&Xkv;Alf_4$*I0GL1@OW9M%N=rk>v$5&xs0pD9pmjyR74y_H8sP5xV84VKa$!(p16@{wF'
    'VTW`!%b$SHgQu3292___W}0Pk{(TjU#P)RVnXgsI+oyk#^ZzJvR&w(n0L45Tw@n+uB=pkz6)ix3;HR%PtWPZSo;sFa0J~#3M=ZZ~mi4}(`r-xTT?%;F>wU|S'
    '`60+5<RGvp#)DVdUJn5wjs%7_Tb`<Irtb_H72fm$*VPE#b3kKKGT$P-$TxpIjK0F(P}h7a{Z(yNn)LAcvEc@znde+-LMYr8;&$Xu%ZC}(N9p1|p`1T`&|xrV'
    '@9n0%xh)ZbA!KKRoeWNdwoF3SnIO+EuB#7Ze1dX(asH~4cWG!l#23^1RzO<pUQXYKmI_3&Fmclrhi<bk9F4m{_FkFG4=ufGi$6-Sx072Ft#ihjz>r9!{G?YC'
    '+Id(*^kJy$;fscYnavs0s-BK{F9HU4*)_Ns^kC7&{MT$6GwsD>R8IK<?^9vh7qFMN=)_W-Gy3~y06TxDK457ssG$V#2Crb^xGv!qnvBo}G_Np=2Weh)%ddF('
    'IiS>&LB{G>eITk83K^L0<ZtM)8qjGKAE?C&DgKGd(;ji87|k&0MFHE4La7)yh%kq?fOQ>UZG<@tOM)f9`e_wk0M?!{iF4iz0besPgoxHS;|U|MnZN4i7f@9I'
    '#gaPi8|YX}y;(dT<OD=xp?7wB@nj!S$KPq8V_RT9k}?b|$1Y_9W3v8p5bpP8#|%VwH_N|#kU{&)j<C&tvd(XAHXvD-az4}kQ$}T!QYZOx8TaV;MS_k0;Pi8p'
    'ME=ZHT|to&e|(EMw7-D;zA%hPY=Q>o@m1F`9g;Xefl7mr<%Zw9>Mm<K<LM7~2HXB%YBDUvD-?2ZVdv)6^H8IH$8U+qA-mx5)&2HkhNxgtZQ}CXIK=PQyIBQ?'
    'rZ1?Hql=JjFAsNr@~}M0YvpXkBdM^o^ZHa)!Qqs04r1~!ufoMRGy9jC*lD|1qBkMD$UG)gJG?PM4Mh&{w)q<T0KI_i-TB%ILuXCUiZ}YS8WgP&&c^zeqMaHV'
    'd%~n}yvlcl3J`!&`!cKm*{q?l$j17VbheDsPEAFnNG&GjZaKA;R-L9fW%Je35W@7&X3=M3hy|49!T~U}GkJA=&wF{0QzCXE6{E<Rzb@ij0Fe@V5F@sklia{%'
    'A)j?28Ve?F|GwrY{d|;WP*#T@NF+}!<nyebi~DOUQ^i559STnL+KY|3sLbDvB?z0hm`J8P81LuR9h}Z-&j5`Z!CybXuRckuB~c_SsXX?}Il50sbe0@Jyq=a-'
    'd_>R)x(g}nG?N44y&Ypn+A;AH?Y{+x&du6+8VCCk-B<09zvX?(bCf#}<!*?wYM(eF(d**0X^Mdy9!jhBlE@}`;x&pG4qmL^(W7#z%z+P#j6SX(1<@1J2r(Fu'
    '0yV9|Ws48}SxR+T`yU^fr!#{El~Uv&)8&`d%NIEDbW)&BVCB0VifvWZ6?~H6d&RVZc*1XqX*zvXV5ykYV5glFJ;S`}ms`dB9IKOgs;0FfVDy)?#kU-n`AMXN'
    '%woMqUPZNG`Tc^NVHl-a<gsNE<TMb2FD*HfLF15vi)OoJ&fD_dgtHHkqd6vE^gNW;>e}|O*K?c_u<xgl%WAkB6+e6Ulm|IQ7P)*?2#UW}?P@-+GmxvmRTX5{'
    '34E<C=JR?zNGs;p4}9m<R736-2fkl8f*LaPW+fHK)Fbh@w^aX(<LVYxQ1ROw&=xaa&N#kHo8f(14M8_^&Kl4S+3}juwJ#t_%bYPw%7M`9gT3`I?22wIW;-$('
    'P1JUs(Lk#<8j{CRR~OGZe4#LEz+IljvKqb!f=ry(4e$FE;4lJg9yv%FxeXDAp-zOS4iXj3Z24;Jr<w9<!x-4V9-0^_WjKP3WSpCd0iQwX40OYu*m(i}gjd%T'
    'oXH}`c0}D9V%+KW{3R(-d@&WGMLTubdZ9w)3pm+d!j=@J_X6ooe&XW{K@KUXk991J<yPUO=DCzXe1#@XyY5);56Be6j<%nKud4uK|H=MY8yGmI&tPhK3A!IL'
    '+AmB_D|2C*P+5H-tnk7CN|6Wo%80@dv;;CM?~Jrp&q$C-ur9aT!+q4iH;AK*_`2?h{=iYNK7S(mxr&|_DxEdirP|Gkl~wyhR_u&K<8$X34k$zysHNpz(+hA0'
    '0xD6%;66qzWzj09klM!bk$QuX<76ac5bk#hKV&n6ER2sXt9UtNoc@UX>rWszf50EGbjsb(Q?;+aUyMWg`L;uCU)fORG*lDU8%efAO-QdM!oZag<d(d^yMTsZ'
    'FFYogYG`<YGD`dccf@&heGrXGp^EOPk>+@o)%Zmadrr)n(VZ0(&<OJ4{MYL_s0S=>#T<yEwZpGpK%7!B5*Lu{_w_r@ngV*uxVcc<mfZfcaiTZsC8(&AI0|iv'
    'uT_6NlYYC_SYUE#qA~g@N{-iajrs*p*75$`W`*@er@Y1Qpa#~vPY8AY-6rEuN!jiR5-)hH`>MW-``5ORGZ2l(YKI=HEvcIIa<+Px)rv(IBgpq_vn^E?I>_&p'
    'x-Dd<U<dJ>hM{``$02b}-y!q&VUfcZ*o!Xuf!KZZ+%DaBrhGt0F@HMGVXpk|3`-M=wL1lk5q}3rJ&v{&YJ;eZJ?x;w=&@~?+E+zOG7K%^AbV4)QAjtL6AVg!'
    'YM+$UB?En}eo<Qf`vvj<VpPgZ`^2pNe#~=;%+)X+j;Ej6QaQIDGc3)1%=31n{pj}Fw7G0xDOa=f1ITVx)m@<&SLn@-5C`~C)iG~_4;FXJUP*i=ZazCIh{_ir'
    'xqio$8wrhn0UV;i=*WdM5;4wZ<qu36*TuC2CLR26J<k!ya2nj;h>odiKPS91B&(Eul&{Jz9TMhp0xy$wK*%<Qv&>XK?hBJHs_ga~otN=H(6#Ww;dCNT*{p%c'
    '{UzH%$_zTQIbXCXeFy+<s?qg@DkxvT4`uFqJ#$NB?)5Tf*@f9>#F$GfX~Db-q`kkvNUB(8%U6SZKXNTEO<!l6Aq>$nB~wp0MFRB##j1Vk_e_x#E%$rI&0fC1'
    'tLxwRgZpCQ1rAUeSI3JKsIOJ0Cw9SJyhx=rUdMr2zUup(|8-SQr#PYL8Hd+F#`ip613Az9y6^@(pfv=>@OGratTS*x(rF1=d7t{$U<nPbn@xYQo-$6~^)f0}'
    '%YUHeD3-0<MFsRiq15q)9=!wDJfh<bqwo;&Aaw4_<>k@=FmZF*!Fyvu@7DCK1K`V`G}Q}`k)xuSwEY>GCmgD%D2vRpWtG<t&U)cMC1mRJag<fxbIseXI<p;^'
    'L;*0(<-_T!!vmD&W|SeaEOUQ3M01{{>-8wFa#S)KzH|T`RP^ep6+ah^9XK?n&If~k7xP+H&=-zx<Q!zo-9fyIXe_!y9PP{;`WJOXq@TNURM{DcQcFm#V&*c0'
    '%8vPoYLtk;ZI)&Bs?SR9+wxr}2U=K|@ATmsGC{E9*jYlDj?{6>t})UY9PZL4H7!ixz}FBC$Me^I?oNA>@ekxraLIn^w)4nFHtY=&#b74`T^W*A`Z%&T*yXsq'
    'Kkt%pD9sF7IZ(u{kGv}H!tI@Ai1@uor$rp3!}Hdrsso13s*sWME?hbmE>ajSLLkp%eI!P*^C^r2md3ElJosgmC3pPhjQ_!YuQ~ri>vDgBUd<}<dXUN=4`nQ<'
    'gg9l2>{am)AcJR5#<<N0Mx2iaWmuql12A%FNGKkeb;BD4oU6#UyXSny@qh&?wE-Nd4bbH*s9Ua{p=BT*O5Ms0ZwF`pRfg`xIcvZkEQNC~V3qA}VJ8rgta?ag'
    '!p1D0%zH*cDys-X1Eb_U^XwqyM9E>md20iDYXgIR`_TH5(#*C1Zu7Oa!zlznnheW@cEaii{-`B<UxjN5CdX(gfKSt9yq%Z6x;s4Vky=@K8p2~?0=R<|cmcP$'
    '1wI_JPUa3+d}o&*>VGO3l-LhK>CAqj-{wG~^JUP>Rdu0sjN7G?lFyiRk|yYL{=Zf)<GE-9ZkC-wupu60#W)f4I#6kBW!x)uBo8vLsz<fXD0pn=XO2OWY+_;w'
    'R4(@*GYpkgi7m@(i<sBXXMN$YO2}~tIrbP&7nJ$uM`<-<6e-##Ej&SD6!o#8JpI9s5bNFADa6`}inzOd^6qPDT!bVR83J-41ZN=3x+fWuawmdZPmx&Dom7K!'
    'b8CABqQpFkV8uP=aZMIZfFY12=K_ZL)JiPjEZw$5lG_dQYJ8%EB0tJ2g>HvJkG(5%%P{Xl^v=QN&2H|Xq1pIWEctw;(BnwxWS;4Ti*3?dlzG~Ia%XK`?R2o*'
    'UkY7;C>+-UONb67V#)5=-duQ)0Mc-6OEG%(ZqhfYveF5CXG^wDw204K@+%V3N!^Rp&uNu=vrXFV84Y-fWVcJE@jSUZGp2J<{188&SiMqKeOJ|Qz3!AJ*)K4M'
    '5$5%zUIhi~RQnq6`aH^)h)E0?rd4;@DM^185v(Ijqivatn@wF6R7U>-vIz&e^bYT+NT@h;7JP7PNUL7KVoiu4^#wb>o%w`w{n!PP)kz#dtQIP(xIS=Fae&H_'
    ';84x;0Qsv5C)~U$;<yh53yx+>oXT^J(iY|tXi@4x%BWO5hrO1+{&)(9*5mJ5pHYGO4T5;?r`4!;wq~RrG$T#QccbiUK}(e+Rf41*c(x4aw_`udn|F+AUkjsH'
    'WSjBBbtaL0WEbsrb57Hr-1N1mz|vIR78#F!1Mb1<t@21vW0y!E(kk^T&)R&PXN%>JH9Vb<`_$Uu3y@eyR9`^pG&a$euRQhqC{?|m3Ut)-?f#yQ_!hOCyDj6*'
    '9^j=aIE+p^0L0W)U0v@8<R_t0%uNot#mv3o<!h7!SXp*kz8geIqkSfbSb(rjJ8*E#-=_i@&|f$wuK6G1nTC=kxpXp3s)0)ef^GA$_ZCxot@kbf&mQ>DEz#;q'
    '9ZMiW#NRD;y5Eds96<*ej7XlJXZmywncI24Dh|zt#8T-l#^B&G)|pQBSBbN?(_Cx93rI4D{!E#=`T#9yfkA4-FhESw;G}aF$i~f`G9ma>7;Nlpe)DtU)bIto'
    'urpyFg5ab;#ht;dj3TH3ODtez*3%_lh4ZfqCotWhFkZ(2mIL8~Es92?FyumHn?LO<%JN&@uwX(xd@sX?9v@gsAV*{g;$~qyx?tTLg<Qepl<>po9PJO&yt`zg'
    'JV<$RHsPdJ;B?uKnkFPy5_KfY(^*fz?u<(5P(nLY8T&>N`E_lLR^({aN<wA?m|*$*!50iIVi0-F%4&Q-#OXPx20S=62_&}98@sH_b4U-^i^pWTozf#Z0UDiv'
    'GC{1LfvBsO^(how#O=ypOk}WrE*)_Kb!j)COk(RN?%^0VwtCu6h2fPVm(KROx}0vJJ_(hOg@NS7{j-wCeVgTg&NR3YV1S{&3SZ<hr(_H&x}XY5wjC)$cb!)Y'
    'K9coJl!m#y=nj5KtW`|z7Wk+dLFtY2M8RM%fb*O38~7xI*6Qj-0n;6!wG$GZ@dG;)`tl*!FBD2Ki#%k<Dt<;?@2-=rKooS?`Mbk2GLHyf%o}jRq0@}a=t5?M'
    'd2x9AgadSP=C`nFi&+eB-&tON0-^^UWaa3VRxMEuR!vAezT0ongwEu-g{Nb}0ZPLWgiI>P<M8e-itS0~0&_ouT34TOLUMJ20`fk&&2;k~aExPH-^XwQLJD<8'
    'xx=Ndu5Zh!k7CuKg8OAK2*z$-?eMua5gUV<<S*4t+xRMk{V`;7f(5!)aIzWMlPE~*Yhf!APzoN5yem<d57Ab|gb=Ma;8JA+ykhY`-PjLM+AS)UX&$SB&sP<P'
    '{vy~A$pf3jsB3Rg96-J9gA>XH>Z*DW@}%N`Qx&o0?|OmGD6jHm4kX1`4Tr*lO)lQ^3SPx+bbu5?4|&zD50+>%3HhsGWMld>iWaF#(AyXUp*=URihK07bXi5A'
    '6mu>ym2?~FlHO%II5Cic5IjfRG<>c@-~>vi+318?$GWO01t-Q3fSikDBh3{O)aQcoH{gW{)yFF0@`?5ph|ZAbftr#bCn0)#yFH<{R8bc5#{kV1@pUBgoj4b`'
    'Gu!2Bp;NbTO%%EnXho~gVR_IeqtMDc2x*+U%1;0enluh{CUQ1_n_><jmTB3z5Re>3wDT$sswyfd!xF_{eO~nsh+Ef_k-zF?kCudvlAFuEDh^gcmd2r+R-d@~'
    'P;vis<~p(Yc+vRfF^ubjf(H(rwL=>1MWlz0m}cVCNH-c9@fNXNUp-PEbQ|LML~!jZ%rwX(*g}wUWurGgb@TYZIU?`eyvTL67zaR<9B#WaF^5O)H3t&SX0(Nt'
    '+e)Z$E)V<-XjHNG<nOY49`37X*6&B#mM;&LV-PsbtGKv~<am%SC}rEzzCoN0R$T6LWE7+WEDq~~GLTNelFVSDC+i$ILzd<Rt{dHV!cP+Zow>uh2X<lxq6rv('
    'i1kzG;2v=FG7i<<WgjsIlAU&i$yJ%&n~AR;Ks0804BGE~AfKFeAGH{fAp~~LB*wM54u$VufaQ$4E1ds!c~bSjp~IXa@6s*C4SM<SG=Z)yv>B;3MA-zoGB14j'
    'x#-%F?pKJNs21`RyL<01AF>&$1;ShGw1e4>D_-aiY_P<o{XK^H^y-01`36od%gC#K`3U<H5Y18J+6tFX&>gWaI3HbG9yA91Uhv<w+w0Fq*OpU^wq(Ph(8@Wy'
    'b;m8V)EM-Znn1n~>|)jrIhrwiO?A60Wxt1qSwsxxk&r@$;qO3fo(d^^g*k_Pts6<%uJ`p0AR3$D+p+L1X!pRN^!g-F%9&-y!?(R-D|G`{>3aCKdrXD-Ztw0x'
    'a#w(Z3^-g8?cHNC5)4)%4g$&BzM5)-@p33+2p*{X$FC71GkP5~0}J7pAL+$wR~{MpfY0H(VN}U}WdBVB?cPX0)_Hfwqm-a`RUt`L-hGnZMIdGSqYuTYcGgYb'
    '3Jy>j_jLd6zy$UO6)!P?7}O>(q4Hm1UPWe9AE|Ov^RP`X9PM2AKQBhQ;OWPLo#XO4fzllF+)|WwUcik*I`cde^+~#dxqXPInAhb!y6nf-b^bHk&6hY;<3#3T'
    'n)uHwFclRmwI;~LB6;;<m)gGoFIHn4;@rf`6DXt7nub}Td4W7}XYrKIiUXBe)9}u>nBygD-5tq~48!Qu1W}GP?5j94Gf#bl9zb*krI||Kg6`G=2nOZUTYa`C'
    'TF^mz*<qQ{u&$EU3%BnL;omrgtRyjneeT%axY@TMdDV6a-EOYyJAi2HMi_Qf?5oSEfGZ4Eig{4imsjxtOBcQ<<O?kyKnJteXF2fK*x4Z+jdjjL=upHA-FaL0'
    'ha2*flsixy8Un{$HhjX@4$841aLipBh&g<<0nN|d$j1OUoo?X3+XzA#QOi?I5Xv7u4Kw@MRA*4A6M7ySCgSHWx~b<Nd-dspgD6kf>ya+1pc0kaYg8q=Pmb18'
    '#7B!?8}kV*=}e0sB;KOCMNV3thS;8?Z;+#KHr@|8<3J_kAP3`*kx}c7c5JVFBgio5=ZG!4n=seWrkRxG(;s#EzcS`kRG@zWSxu5LyyW8a-fwqbq-e=A%(_z_'
    'G995$4sI0DH+7B@^|tddk}=yScxUHN&<sntP05IFkcW>51(tUP>@x}t;5vST1i*03#I#wH6N8)K=}c4wrHE->zkuxpWt*xaLyo~>*gqrA#;d-40W12p^#zt&'
    '_VOf9DvcGxf}+w0xA@<W{DQ+MA%`~-ffjOn<1Oi1b^{dZ4<5saa8A?1r_<6`l!J!7FZF%(YB0eqH;PXdep&4MS0(4X5U2Yq=lnDR%F(=YUO=G|zk8#;Z}Gdy'
    'F{Vm`a^TPyl#B{wnTOu6nK#Vz7vpvfI$Fu%YbxH{8|Xs#<+~`qLF}2t#t$%TDQt@4aNDdfJ%Xbr*rDD;(f3W1@aB?jZ}Y=Uw4SYV9>Ll$>4+6fUI#y!@X!y_'
    '!a=;eo{*&iA(edu7RJrx)oyY9{io871ENySayUG%3JJpVaB6mm41H6R=pkC1t!k^NltPbUOJZ3KR|bAlbYdI#AB+xfu<(gBft0_3I67594DO%Pg(fh=hCQJ5'
    'N49MuUHB=u(F>p)HP5SHmk+wAKv;>{i`Ue$>Yj0Bn&8_Ia}<MKpYtY!Wd3$1HTkOH89QVDzPjiYOiGcH5oU|PF{J~aX#J~5H0A}{5`$0F5cIQWe&htHFpLgz'
    'kln9im6p4VSlanC4i}cZ1__w^?#k^g4iyLJFgxM6#}{|%RovhCuwCjPI%aQYx8n{;ck*M)FVIT-I}7+5w{dw6>HuRDsXfc8y}DH-dfl*@z4Qsr`WuuyqtYo1'
    '88KP=$@;#~0v<?o=48-u{>c*GzYCGEWI3SU{1k()ko3>Ro^=4grFFqV!)&_!+jZ4xixFJE7~x7+PB1Rdox^<ejd)#Mo|LOkx{h#R;Y04vC#tKwnr5RuiL{y_'
    '^=llfR~$XteK-<@U@@}WuKAr2;Xepn)x43v8iYPOe9=7*8<FfEnC*HW(>2Z`Svwg&KcO-@O_yqdk5#-*c{#higNysWes)Nd_y*FiAPQ8-$7+Xjx-pQaDaDrA'
    '%o@lOEZy&*ng7yik?qmQwO0$Se|#`@#+9iLwYpzGrkj(@J9>!-_s*)36A+z&PZz%^Z`^&=J_6c*`ivszAOu@*%%FW^xfvb|+FBDYc9+)G=j!*d!`dxtHkPK$'
    'zGh=#`=Q0eGbbP#&Bk^@bqiXI2i@zsWE`ZF^B8isWdEaT!P(93VVbv1=%%9OU&FV;Fd!_9)Yt5e;I=!cwVUY<HbR(9S5#PD_XZ6Sbn^z8yLxdyb;S`>h6Q);'
    '5M|8v?S67qYVlh?O??t*F~4U{TFkyzFMM}i<pji-8$i2D#*R%THvcf9BPK|BTt8XC|Me>%2;CjWDl8r(T2UC?hC~=`$W+JWt~|nskvO=`t7XC%?F}X!RrQJ$'
    'VwWkZuJTo8%a~1N%3nfb=W5PRs6e$>8_LAbH&8J4Z%2|3-baU=%F*UerG_){;y2Kz-Nc=j7J6J=E9@g$742sIu5^-S%!@gDMZ{epQ)x^b0rpq6aWjX^0r!6x'
    '=km|TQxybyco0B?tRq&MCVFR^&<Th}NvLf_Y(a;wxF_as6jWx8s07|LkTlrAd{x8sOW41nFnYj2)R1izFG$_+FL-noqjQvsie8L`zaRe8lFtnF1z}`VkiJN@'
    '{2qSJFucl7+!<*HW~G1ji2nHRk;H?AR|lHR2y|1E{L9>2C1x^e`$sS`C3$r@q4oh<8B(z=+5Q{qyc(8QeAGv&?8yGrGiPOs*<X@FrZBSyaGN_Df&A-={JR-B'
    'P-)Ca{;rdy&b>*g?W_+=gm!bw4Kf=uNfaZ#!QfXgc!BHr@|&;X?B>uWQUM?R_G_5-R=#?Hrb4}Wfu@drS!63e$*hBXb4}teko^!cKL*(;AqP2IYlU%5ZN0EJ'
    'n**L(K*&WqAArlSjQR~wN|D=A&iKSZbRZ#>N!%nmK#k^+F<B=hI@D+c68yS~56L<s(H%c|M4*V;zm@DRyUyEoRN*CN7WXBt?}if*3)?V3q7mTb3=|axDiO0l'
    'q;{-!nmyi>Ir&wm?l-2c2mY;Mvx-#>ced+6@+YsZR}!aB_v~L43lE*96`TIZg0rdEol~1iyaj#Kp$C>#Jg@o(oG8~aU<@Hg*@<$ccW-s{-nE?$Q-GI~NqvhL'
    '%^UMlgE+Ssz_^HpH{((NT0b8ZJW!F!jHEaYiZbuhfV8(8^7W51^Ou_p3Y5QrK@E^=PttzEB!UG`CZ;l5_UNW#DNbJo<jO2eQHA#5_7`M_uGXpgxLx+Ay_Ex$'
    'QiG&PyT=P$1;Z0(zIpu+&X;~1sB}ju?j3OA8^otP+XIOXb&3qiij!7JOJMtZeD;8$Q<WSU6d{AeaCmd|*a1WhTC|XUP#9D}yUjwzZG*aCnT)9;$R`ffhHFQ*'
    'FMf{^N)bd~X5!fKuweF^VRzdmg}^5IslK2SCXJf=VcIIMUY1dijkQqKu$w-hygqg1Q#=M`{C&#z`@{P?4xO*NgMu)E3~~9@B0q<9fb;BZNyibTGxRj<eNzmk'
    'V4_0`gf|#--<{|!X*u%9$UMSkR-dFYEWDcz$@5~NeD7{8;bOpafP>JjS669VMYg$$0oetQ#)rt;|1)1xcEHjU*KnF<EE>bS3Xgayyr-~riMO3l7)VA<%bWJ<'
    'b5N%#VCC2fm(o4B@W~@GY{mpQ4XL+&+9}Vdv?6bq;@{q2=i|H%wpeF9rSqQI0v-%i6I&I7@)r>HSdnDV^D1?twqr-}Yc)TBXl(dzpci<zUxFq%<`N2R;Y9?4'
    'qCs0e0Ey;982Q0QrR$;0iWlV`9=QmG?#Oxx@@A<~x(J26<MNpTiUSGhP&+xrd92d<vbX#wr_>G@xG9-XSUos#rf4Dqv0Bhk=(*lk8wz`AmO(0HHa>lVr|I^k'
    '(2a{qFMuczI%1*uHS3+jj3Vex92}_0PR5DXWhy>6b{0a6)}FYtelAnVsO%jt&@gcbf!vT7on!PC9McUK<?mh~jE>JdOn{bm=^Z#UY99A09Fm3J)tav^_X@<Z'
    '<$aTd9NW3RE#Pi7)<-du!!(b{vx9(kUQ046qkDlgGS|ralzQVk+71uvLSVap5+O}rn|D;Y+R6L^R(=8eVvz5Y%t4#EjqNz(7P6Ht6ZdL4kg|&=78(kYm86s1'
    'Z;nrUy6Jmzv+m)6NpsDQ!wYlVvt#CRmqz1^_1nd5+pjL%p)jPjG8g#qkworyh@&tWHzPKe4Cc3VavnHzhfgZrlG?0y%nw~64kV;Pjk5hK4A6MhAC|Hx5tC-4'
    'w}|tKf_hunY`)Icw0g((=U~wOBxQ$z2D8ylkNTB$>U+$C8&F>5D-#|O%%&l<IkVA#n|2KkP>S7|G#SJfD67k-xt(z6FpD!`@~XQWP;!TFQgCPtcLpWh8qzAi'
    '1q%A9ABUSC>nQr|(}SP4h!vJn>OrK-D68&)uu27@Fyo088N_Pfw2IP!d6qj6ZEkpQF8X*w%B`X@x;K!k&C04>?xd6lY3@FmA=wDkW`#c)ubRUhL1e#49YW#{'
    'wzDovMWxdkaWMD3itt4%%CBLeg2F0A9>j_jI7q-QGt1;*yKQnHV2HQqd4{EvIilE#T36|3nSTKqQQ3S7(DrJ!pq*4y_CAoO{xH$}PHjpJM{1KC1r*^o({B3F'
    'L|_CD<?~h4H{Ez$(1GqnGUcYE|7lg7h?`XTj6|dILt><~>Uw78<`G&Ihvr_9zUoEVjJygkGciM_;~5645sO<iuf~TE>vrW%DRu7lX%UO(=Ua@Q(L*$rw@-)D'
    'Y5j^y5g01u)F{h5UiL-WKR|*Itl2I_o5PDY`~rfmWGL|_<C}Zdzz6Jw%ipXz{!zO7vk6Qbf{C9K5KmTJxk~{*i*BVCaBnx*_61xoFW2_4cnS!RKiJ)%K=3|n'
    'XQ)6B-xB)m3q+}5;XY#c6AYt*Jg6^EtNc*6WJRJx9mE>SyoyG9yO>gL47w`HLJBS$bo!<(_1Dtof<kG}n~ABPfZ>7C+VUitgq-+?+EHce4ow~6jL8a@&SMpL'
    'T-8K>AzI^kJ8wLg?euh0_TvQV(&SZ$-DFT+b#E~biHgT~WZ=X;LEhkGImijt<*}c?>xBqWUX8uNn|4~zb`3S#{~%4rie}PpIf2ss4pNm-*M?JJm>23<&e5W{'
    '9r%<#eWXu={#~Km36{~l0*-d>S9#kXckW46s+|j72yoTaus%OjpQWhg%im?;B19e9b-~4ruo)cOXJdsBZxA|bZbb*cevMp(k5?|PO-+c-gfw8q8lH0kDk!JK'
    'A4lRx(%dlU^#kB<5PwvQAKehXza-PzjD39li6+kZ2NjeGf99Ds0)B~w`gSXWts`=USh9;d$Dg?jb=mDs9WAhB#~AD+zf(m;`WFbY$lc|obly|1IJ8>gh3ZNw'
    '%-LpfgH}Zmlz{g<T>t#ft6m>Q{B6j}35VvuKf)1Kzzp=rz4L^Gbg1dA*{wxwHDf(aLotGlvtvC@u#~SLbxhL1UwwUqZWj&b%V!9D!}iqmPaY6QFW@C!t#g>G'
    'v?8WY8}JVNPLH33@~yh+uYUn`GE7Kg8}E1l)>L!Ri!of<1ZkBY@q7;?I=yeI)mzlq3mM&S)e{QUF_d|CSv>jyd$qXtX3#4PuS865af22y_0qX_4-z;5(cJ#2'
    'G)VTfnf85s0h<0cKb*nYu>a!?-Si6Q!j$nVSYcpuU5(?)1^A2p)O@@B%+cDPP$}QQ$(xh*0#U`a6ph0e4A7}d?pG(BlnwIvt5SRUfnM*+{7ijYfq)Wo6pGO*'
    'W`4xFL`^Lk9aPmuMhuT!-l!ddF+@y4$e&&;))(A877?r?3=DBIEpDbyn>x(mL-=;6*5PuDz_uw%oM?{~5#JzvtJd&54~%5^XX2KARFtCe=n8R?zauJoz|x&f'
    '&G59&D}-kZOEcqbB=(&A0O2sotF*Wy^z<mrZPcc8fv79Cl(eW5w-f4dAknBt#>IlDk5y%B_RRhBc$8AqUN+;lZ9o%Zh=GhXk<z?+hdk5!bU@OXbX(FgBlNVo'
    '_Nn4P9pvN;v#8rZm;Ghiec6P7w0Tz+ARyvgUF!*zPS3YHeZLAlukxF1T6p0+kmwXWIb%^)74FTjy+87qaRi;JppmuP?Y$1X{EnPaD0RE>`WhKymrLt*FE+y<'
    'C1NK>5N*k2T#TI9tR8GvPMf;n4aE7g@<2r@y~cNfo{r4@>XF_j9J;-TUUp?|+eW`oPrTB5{UFJ1wql<03S(lyW26TMv)$`w%R&xFN+k&bFJfK2AdZDUHlZdu'
    '(H^P7iIBvBN~1EaaO8mW3%I84neQ+=;ZS0ZvN_E5JFLGVXBaAT=t_J|l5*w(Q#Mb|Sil2_PR!s(Vs6-p``wcqY_uN9I~T#hTnBswMXD;0Ogv4)7jRcuWwIeg'
    '!Vp|EE+9|)v>6rZ)J7c1NQGb7z!QqDo5@HA&#`Py(kiTP^*ABXX*wB!5wWhq1J|95S!q!SMs}N^H<F~;oX)tK9>gOSK4ruSv_^bqzZHvY{NTQJW<)lx;ThXD'
    'ov9kGkW{g7zn4n}LHzqFe9@Twt2I1e=megE-6#qi>GDpWW~lizfy`U$?1;EjtMR_g8jM7aW~4BEc8%MOp>J~rL&Wccky^y@3j)fCD}-;!=LcHCdyGp8=I7PL'
    ')4J<(RN;FbhN`$!1uehxus%|CunqTZN9H`f;DS%nCQd&g`bm~jl1PvxErCG?0`L4QaG5jV7ytuVL~G}BCfxGTj(!6`a>8N6C495a04dWA+|o_8h^<7xe{&4{'
    '075z~d7oF<f{vQ1PBiHTk>~`yck?F>w<8vZLT91R?O46K044rovmKS?Ga8;`*f_ybZd8_QCb%t=U(~mM0;1HL@a8_Xpu^iRKB3tLj79IP!C=Yz8=7snopd>V'
    'g*ZA!n_Bc$&Psle@*Hh2sW$7~#M39yS(Udks?_H_l6R3g=0C6$T?jseXc=HWjm-K|{mT~+haR4pv$r!8q7N~70%#{oC%!}P-+on}=%2DK8Hmoh;Gm2=uU?js'
    '1x6ET`(L1Qft{O<*&e8j_6@|`a$cpiqYzG#(&<SU`CCo6dY!+eF7Mdc@5P#?@JNeoh2fNlNj`D%YS729CJAEdBemGAe22?}Cc-1qw#qx)&eJajQ^FpW;Rhmy'
    'q0D_zK?WmVAwrvG6jE*I$zvKZ8502o%_iFpP#VJ>cF#S?TGC|@UZ5q-t!5~4FYw#ZF*d+F(n4?`(Ht+ar8as)omazU+9ZJE<WP|h7Q?in=f|oPeJ27J(>^h!'
    '2RqlcmN&~;Ho>6*j#N8O?y!p)9xC621<veaRZl29(R&)(Sp5iclYQ+7AGN;8?}71Hig+}ah!2fuA>$M7R7OEc8Iycmj;Pe{tY6X+WA`AnfZbau?Yihy27+3('
    'a57&;+=BshHPk4L0jyUA_P4x&(-LOHHdWEQLC&Yp`s-PKlvj!!VF<cwKo<J38IafMIf5LxilvHgEiu*!hVHOO6lQr)htquF5re3i-jToRM;kL&xvi!;J^@kg'
    '#X*^D6uL}W(zn-$v=AIk8*tQC%tm<<ARUb6w3^eq;!vJBdDhPWvSO5xarG}$p2oQsh`Q29FuTVqRCttC(0fw97Ijn{3Tu+Ti-I*ttNt=Ch<~q&4(ENG6?wsA'
    'lp>dN$z|2A9-v#FggVGz#L!w=4VR_i^yh=$yc5IBHW<@mll&^sRzN95E>790t5$zS{<TJ;;!xB`r0>d&8fldklYy#^Z=Viu^1@Rw>2~RWnxU+^N7P(P#*n@m'
    '-zX4T$nk+e7$+P$H9?t%sjSi?Vn!toQ~IhErd!ule4wkb0#V9&Cvq1-zt8qqGfn4&Ly38C((Dr`U^)3xL807$1fn}tR{f<rg|8n|Io+ntb^@h4m8XeF?$j}B'
    'gcb*;Dh!RC{$O%#A=3kase&RXWlUmaTbegv-hKlT&FkihQ1F;|UWsQ@3>B18;xgN1Sw(%**Y5%Q@+=kGW%+Ixq7F}3Z79V5_kmtrLH#}Ck<sSM3gk5R)F$Hm'
    '0G_uC<^1&q^mhd)6gd+96dN@UI3&;}@`jMzXhUWiw`VpaIQ@o%_wLXZwSVjC5jv*;90lvsr!#h5(K9AenZZ2nT*S{kgK~Eqv%*k9W-*DAhSiu}gzD}RlLCWu'
    's@=@i>>##R!_O=@e@x;!^}bH&IT~%aCy&#SZFpW?UzJ=QrL*EVjH{tQC(_Ct1G9-XGJ-Mw2D#%GVK!`;Jla?Ay(#DS-*}FBLrSr~^Ru>?#Y*H;+1;t6ASLHN'
    '=MkT)?zCdxVS(xu0K^zR?Chjh+=?lNA2+uaC?@_k8zygX8ItNK#S!<si2nAHrGkT$kb}sVm*>9#U$&W~z5#NU$oSyHWcg3Q#5(w!DMg{;3b3$gT+ry=-ak-b'
    'k;>8xm`BR0U!NYV!qQ1yZh0uHPh`C-5T%^M5Oh#X<=p(lh?qxPZCXD|UQ|J;UqH$u*VPAHf&3_ipKgy)Ib9~#V-?)hBrfn4c(du&^5%%eNxeCt0__|0S)%PR'
    'm&WKrMA(ZF=|L-)q3vt#PR9YWovA#F!WE}SUAkPycl&q$x@uR`T-7I`QqEExXI*^=vZ{2`z6L3c)cG;US}BVO*wgCr8sGXXt>qKP`q(n6=9(I1yL@g}Afo6I'
    'J;9u3XkQb4bW&%5oV~94^;~%;B&0GT5Qb8ZZ=7jsQ3t)QkW|uptX}I%a!EGd#2_RR9dlPbb%n<|t;ugfgEukD>rhWXG)Fw{$w29d&#S9>FOCNp)d4@4oNUW9'
    'pQ{4=`I~0NK}yIh`y_d)FD>t6JOR=8Bv_h%tOPpr+}AeUW{C^)vjK`zG20U+QsK|`o&?R#N7NHX=Yd4GAHfTKRCSfclN2yf7P`sl2`4BzVWSkhvSnb$RYzl*'
    '`fPlN-nleT9&p~KpelNmzv_k3>bmO37v!_9{1`Ai0nu$xu~GN5y12OQc+$D4d5k#-brN|s_C~dSaop%J2UVd&{VvFIj_dnJMaYsmM<IfLT2<F9&73zFRU!O*'
    'atERCzOH(03C)cAbO6!VJ!(6_8qg^ijaM-qvt4hD=jZ{vjf2YjFJ1va6<h0pY+r#S#)2_5$JfW?0pu3YR_fBYlTYM(&=QIKNzxCvfu!Y)KV&J!0I98@SW!z@'
    'f1|4V0kxTr25?R}SoeS($v||fkvt?RnzqlgSkDn!aez|JekkW~$zL^HVs(O{lyZ=-1k@%Z)?~I%I5hju?rfh<)C@3+esgPpt!K0$nqYyZ^~AO$*|MHkc?4fx'
    '1!>0jd9Yf@V>8%W2J|?Yny;|D$~CbqM@Z)tc)1Py5agB2hk%z)g8v#Z3IsE!RnXnQeq~CkPjX7Nz(GF27S^csMmys`otUp7$JMQt-$EuMAC3DEhpVG8)x`ve'
    '##pzTK)%BOl2IO%uJl{<@4T*rGcO1SIlle`!}IA$*LHyn6y23vwe%~1Zm#csMR(m|dr9a)4vyqiySlXGc#<x#?OOVX267&~`@jYG075F&$W^qWYOJ60Zac#y'
    'Mp8~5E{nSn>Cy+Jzf<wF^@wi%N_N1~ZO1$@K~xm(m1Qg_aX5hJ6eahjlb|RVdx7oyBYy`DjqmMBJkks>$MtShevsE`cP>?yq)gDGm1)Y4O?>@9hUozD0ZJ$G'
    'CgJ<`0=_+n79KSON{ix$#XEU*HQ#rA5GoNnq202sK5#wA4+0%xFmjbZUX4#62BWM7@Ouq}7tWbumCV{X?x#m-P8KiIG#{&Ame0zqK)g=SdB}$q1dWfhiJX9t'
    'O3(;zuv*ZwbGVhJ`M{yX?BB|4Tg<+9wk*Ny-#m!{X5O!dlB=|@o~JNMHMlWHo{S4|&{Uz;lmnIW4M^US`zqiAfe|hYz?#4aw`X39+4k!CZ*48F&ru!WoFZcV'
    '(bcw090Bu4!QX)dRASC0eUg~_1%C^YU|e`hbh?3qKp*eS9M`1!o{d2RXc%pz`ST{Daynf}7jbz*B0W+umtlA%;(@!z#XG-XabgnyFtBBg&O0~ju4Yi6#2?>s'
    '(5=K{ulwWvw9SD-W41#%TKX~%?W}hpZ9`<}AacU#J54A9ZxtfnzuG6EI<2aEv_F+ZZCj3K8J*DmQ0Uz<uG>?Rq0G9{n8T1NGDm_AJx-)Wg61y-9poC^Fetw`'
    'v)3Xe!uMdv2NSKG_rOD2LNt7Z+$d@V!EdWb<HHmG0(<Z#M#F%>asDJdQ0YtzkbJ`orJQ8w+iq`Q*hi%O)1sgEZ4XpBH5&Da9IJLIqiubVQK}7^5#&A-#DW`g'
    '=rSn?#yn52BpEZ{Vltc)4A6;r(3TL|%HDoGu?ZLqy3J!@LlI^@uKlUHR8H8dU!n{@K9Lp4C{zW|v5ZscTf<oHeo7VjxqbWuL}LIyrZ%*o-KAf9N8mMfxius)'
    'J2{LQ+y2D0h*^nv(2o#z%gN&@v-lUi4G_00Tl8#0r1@IW*s_oe)g)D+L{aDfUr-&-bTP-aDrY2iKso9Yl_aptV8V8ZU~&L<)_%uQgrhkoWAUFrQX7)^*UmIJ'
    '(fjDP8_?#N)LZ_Pk9)u}I<@g~T4(<ipt-&^qfb-w$RKa866SX<cx93&&ycV({{3uNe(E2iER<Rkh1oi`GjO8slUtwS6Pr7}9r;9u+E27=XH-g=qm_eO`|2aT'
    'i5Z7eLiVFW6#Zo`FlCI1ZOlI&{H&*yF&RC4H-g^@C*I%FpPyvakYNCMa%%1uG6pSR77wFDCy$pb@OR0Wl}W*vf|>&vlX;|aBR@zZ<RnJ9ilFd$UyrkPE(D}p'
    'D;=ge6ACE&27mX&FRN~SZT+Vl<?|*_LXgrvX_YUX+s#WM<Bp$T!+Z{?!ctt6Ge{}6pH^efnB1-C1qSF8CDl+k5iyuYD!t2-R8iXeU0_jBQoNHNQSOXGrx}R{'
    'S2ieMd!w!vXa-oF_f)>hq6x`SWvkT;p3n@dti`OTL@E3Aqu479QcBs2*|fZRRU&vxzh?zPIx+K5nOQN(9KG{c+*D7&&D9qaOj^xmG1-s~_wNQW%-9cBrWX<_'
    '6kGMnDy=V3syKAJ5ywuHIEDB;@Sdt06=JaSYJz$n3R$4^%&YI+i|fizDiV#NKuLG@g`SD)-4$`IMYKR12@GKfbC;V>E`3?%m+1WywXk0z=C|ANr@YrsD>@*f'
    'Dza0wKuB{ZUUQIi<jg>7Pe0r4oNfZGu#8I0vp*%if|VH6@3rC3`gLQ>L2TZKof1ypqy4i5gmzBcNVn0;p$2H@8ZAnY6c}EOSUhrek}zYhG&@8fW65$rZ=%Q9'
    'wzGl?l`jy)3Y4@O=jH%?u^BTEonm8@ZD6KmbC*?w5D-k71q3nTd=+7N1EmqiI68Ln>iVkn`U}=raonqoy)1Pr@d2O5K}JNd01(rhE}q@0*tP`8GRIL_avqVO'
    'a8wL$U*8-RN-iQ#Ix48HQBKtGMx>X_tKXQ$pKFvV5Kw6h$n2hFzQ6+sk%EJjat8U#+~*5ih3B`cW}3rLrpyKx-L#qjC{NIRg1|`Pp_jJ&7$Zd{Y_9nB7SS1;'
    'ZTByrwBnbG+P78uPGhDSU<)9l!`}43G}zI95x|<D5v)KoM=9<(3R&ODNS*Q>yW;+Oho?%z$>@r!iVk!EyTVg_MdO_k^EPZ&+yqX#ZfO1(BKQhFi1-a&PGh)B'
    'OR^v@kNylxN?Pu#Xf7iwvp0!~;`+GEUyUOvrwWwi1P~R6(Ln|y#J}rmxEvT^LjAirT7K@nc|vt*OAfIj$wIe}P>@$7sMKwcW_Hz8ca;Vt|N4n8-17>$nO%7U'
    'f^cdctNH-*ufrEB4yCqVLPlIoMRYr4{mhFW&zi3hqmUlXtJIru)tHn(>Bh$1!FEu-s(WUX1U1QG4Su|w(Y1NA#zmmW!Nx>KPIsux7cUS;Dns2u_M0z26^GG7'
    '4x%~CjvA~@!=7;H>|b`jtdqs;R!?a^;UEQOlqcxa)$o7;MCAmX{i`BNUtL{Z309xvl#r$V`D68J_QIwQ9-^`NlT4hpfY@!4lJHxhT&Avu2Tpcu6C78N;}CMx'
    '_v`-Hi*}NZ#tFohCZz=(E@?#QFBPgEe;{^_%J0In^}$a23Of~j-w5u58PB&pVIm}j?fYnX)Ai`%EpHIti0g)Lkn%2)Nu)w{aecLa?blaZ>95`vwgu87#mDeg'
    '<=FlLx)k=}$@$~W{saURT0<?VVn^owvgqC!*mORB#*T~it%c9eC5_Z+kD;^t0^gK?|E?$0(wYnrJ0VVZAnw%4wq+{88Z;h)bDqJ_WGd(cOXF~5vBFU3xSlbk'
    'K1&zH%5xEnh&r#X9X+2PWK@+Ct_&Cuy>Sw#Y3@$@oPg-GW8`!lt9I>Zn))QK6*LQ-j{9oEGPY1RSk>8BK?PCFXnUKm&Bq4VEC@swz1xmVnzw_f;>dGlLW1aX'
    '<@pJ?obk2-aY{WwkfXE*=%H@o-)6f#aXK$!B${oAJ)zR=&Zd#Q^9Fu#$y<dHl#0SakxgCY2kxEuNvMPzMs_=|Ds_9t|2`h2)Qm8aGnu#O{zA<NYB7^U@K;xP'
    '^$@fZ4vpEiGB;FSrQY0tc358UKtRE7|3a9RxK&a^ReWKHKCXASZOe;zff&4u*h-u&_of1^vQ$Y4**_#?HDWK65$09?f@EgK-W@=UZYK`T6)Vh=z1`l~ZW*#9'
    '<~ZDa@iIbklVU{u?u&AKA+N^Em`>tfCF6h@0Oy_5{8-ZCmn6=+ATDmY?#~9Y%*5MG-GyUbz;DGe6_Qf=@Sc=xq~Db1Evnnd&mlcv{{!Hg6&ygBVX$o#V~f~7'
    'WUB$xD%~`Hwutw6F0^=nL<It>dzUTkf&($5c|^=!d26w4w_S^vzo@L6@J$aKpptUFl1s;UzB#UCjXA;49pZ>`uxVdKP13-#bBqomm}uwKq@g(;sR>Gr+I7`G'
    'R`FUNsdPus4P}2_ZEO>pbMzq-$b`wc-xZU;1-`p1CA}rC@EznR=Yy@#uG5TD49T1F91?jcZa2s|GKGrDstmbnD_q~+z?H<p>eiPN5Y3(4?YR-@^l@!AWCdwY'
    'MWH%;@`y7K47ndr+TU4ad;(&0QXYWLt8w*s_^&ZHd65P)bn~b8S9rOWGH|3DFsQ1_t51Ye^OHaiIesbSws#8<4yv*M!A|c+K=p3S<;ap^r#cn2^ZGjl;t7dT'
    'wXHB(r>?SgX4}j?;Q*qu)DY#+ncK5dpG`Ook(N2w?Dn4y2gG+{m?|u%JR#A@XS@^Yj8~YHEG(6Rr@@IJL#E!fy_I?@*zwVzcQypX`&`-#z^LD3U>xy@33T;I'
    'O5=z@E}L?KPq+kfP?k#VpuUFOl)wN4185P+8}k~>^dm!WMWug%Geb)A0%Rn{?E5WxMxwdi`%*$@1+UQ9%eM2>!5uidP4<i!(awkL4^$fSzGFlwsN!2L?uBwc'
    'kSOKuhjRCCfuePH9<L?N&X<INP4_K7@-nlMaj1;9PqE8^c4Wv`Q#~$<Gk(yNj7p~|@ZKfdz5!oO6Q#f4pcbrqWBqJV(g8}TH7qw?N-uDGPAxx3t2AM#=i$v6'
    '>njbhGsh1Y;OWazey;Wf1}Un2F6AR(tc-(okQ-1fWGhX`Q0L@TN`gk2Gs$1&p^W1rGEPtOO2{NPMeVDdDNXI2ImAt$WBz=NIQqFm>HwuPCBM8t%S~>OT(HqN'
    '?~Sw5v7xVDK)g%j)$2+h3y~8chqu5c%o|4az2Z0cAAI~9YR`da^BcEg^^P6z)eDGW7hm}JhxOj}&)GEM3!~w%-AwH*o+GMbFurB-YF}aJARV$r;HE;SXhtA6'
    '-(m7G40!koG$>Qf!*F4J=GM1^E=PJJ(#L1SxSl(?!cfIu@+f1H5z{3*RJz!~W-1NZ?f<YCX9b=yp%OmI6b#(nT7?|$sY^2+6H0$~dJjyRUAiMgBa8n!zPq`Q'
    '+X0&h0E3T?<9rWQ7F*QGzv&}w@Hgzz>vPcD<=~dE^GNj(RQ$8iAew*%LT7NocZmTmJsdjl`;;C@@e${ujcS9=?YN+!wnG@1$63&N1@B=N<lF>+TgcR#v$~>R'
    'Z5NNS`HPE-E26dv)h6%I$IT5h%+6ast=y>X%-jJY$nae+6|<lwI0G?SnLD`vk=u%qRcC*GkW-3z5dP0ot1o9w6Eo+TS;kUy{)Ww|P6lv05cBf|grep<C-hYy'
    'L5Ddr5t4wJt{JWG>GeK<fJ(P%(0Q_t+7@b=@2q0smc&t(avoU@BfwW_BK&(x<uc!pC&~{t$~SOwE;p~nCq1kDNUijFq%Htb@A1v{JCsfgjs4Cu)1|A#K4AN*'
    '$D96wjrDJ2n!QRKm^98+kSTL3epo(Xx&mRHqGSSpk}(TCv5gDvZ23!$r$dbgDo|++pnQo4?QLXHJUX<6SwoTAw^Hm+z*h|Dv{^=W(SxI4Jz1WvG!HHF&sgGC'
    'rg4q?<(n`2Y7DsJPi5o&xv!=VUl4z;@0P@$u0VE7IJWs*#v8WmJvp~sJn06F?^T^k^2b3%T}bZd^?ch6dbAPQkw>!A5u=PLD1QMgmx_k}_TIKvmfXni{3;E-'
    '-9dv9K{A5#(z9Nyu>+4Gk9S`f1EDSTcm-&sfuu3KF#O$@4_TQcD}%)>*1c7|IQ?)(Bi2zx{tw9v1|#CPIiE1fO``;+_BWr`id()LxEVF0f7s%l=MNAzUktu|'
    'a$V>Huw$bKwH9SmzWw2-e9_b;SYe%eU1qmNTfHK)n_ZiQ1V50O&+72|waH@W_V<xY_w3p%HqB}thMEVh_rXhBsocz%Q@rCRbS8J?_HTCtEnkImG6M{44&P}p'
    '0?b>?>N{tHi1NcyEiwVgr=wf|WLcls$tan$Uv%I6j=26!XNgSu=EW*hZ7z||$AF^!pXjA8y1B(9n6y6VA;ggV9r)&SqTP_W5MnlKWHenWx?Y(k$q+LqG+I5>'
    'd>j4t9dyHnaGcaRG?mJ|&7o=P^vw!vZ+5`6&8)r~yWY#+th;jP!@|1FX=8?lgxzo3egcU@=$rrr7d>sUwCqsrR!ykB+em{KPx#gV=gN*ro299*>1{skAocQ@'
    'vfDvs(q^srW<zev!<UWo5D)YwzTU~p;>%_;Y33qj$EB-^hllxQuP8s<?U+<tZ?Cce!~LVnAB*5kbfwrjZ{7Ok&t?_Pi4I1+{MjDBeEaT9_mgvIvQyo_GK%!g'
    '6Ph+Nc+O&k(JVP{`S;@R=Jq|4v!Z|>Kv|0TIHLTb4I6Qa5bH8cmh_oiI6CwlBKMmiQ&SmoYLZoLHs87V#P*dmX}ec%dbUxT@BR0DPRDM^<Sb#e+F&CRKLqp<'
    'BYI37r4aGx-|#F0;+`ozRDEbaz)aIw4#9#k7;Fdxt2^N(^Fc2>*vyy5L>Y*V35Dp>3V5<F&>toX5z<HXbhnMQOyjt5bOtR1a#l##1o6v3<jtxn-d3@3ux?9m'
    ';5+1E?Uu}~ThKf2Z7l8Z=*9=h#VHS|n;nA->xZCX70>D()GNd73Cv2oaPpmcvtv$m94=RchxnHDjFrub$>oV|<OHC*kQel{r&X+wX@gPU&(UjL%3f|TPVV5S'
    'qbLiV7~x^QFX8?V2FL3YWD-nSKC0EEq)~pD-(i0Q0>;E50wZScPqDZc;%wj4-OBTWGUmXH`R2XycQa_tWYIT2R5HoB`(kl|MuKyQ@#XJcuoD)eW;DkU=A_We'
    'zeAhJ&5W6Az|AJ31l?{H55u!jTk#mu{iPEl_yVrxZq`f;4u>%~>_bcE-PR6I%d>dG<($vmEHaaipcRyF+ok#D9UXTw=1c~i@Vt%kY>eD4=?pDa@S)LOH`+bS'
    'XIkZG?^d|E<vB=s+eW`R(s}zHoHW>}z5ch+@a^9^H!~(@veVsH+}bL92aVmN`&vvI%;NA6ahp!}>;j~e!uUt9xc<%oKRu}nH&}bkBWisGUVXO2o?twjCTK@X'
    '@x0FG_0t6H^im|F-<0Ih6V%|#Z(esh=GGz^mAaNj;j9NwpD_#08)u>mXn~#3Yo1^Zy0+PPx;T9%&9~hYztLeB`yvu$niP|q5-Z$C<sn(!I>W|B50IQAUR|2|'
    '7f<AJl+XI%BS`W^hL4as3fbTRtsyXI*e>#(cC)4*L6ip5Wi(7&5fTLy$6(W+$ZI!*yHPkfi-L0C&!fqK_{Nu4yA^iMU?3&U?KX<vx_!WIhD=%<l=4`A1I|Be'
    '-w4hiS&kc{#F=hibSG4-ljcTs^dJaL^tgWzJOhFE$9zUIm`EK=7Rj%j^DC=hYPG6Ywf+P>ucc7zVdh+8uy#AXTQ&PD(7FqkQChyxpObv5w{ZEWA1clhh;XWk'
    '4A~N{ykvKqX7y&}I%OZ7E^S|XCm~PWrc`DPH2;oek^5J|yx=$#oW42g>P*B(f`b_ej^Dnl@a%W13ynkH3pY;kqb9L7ZQ>y~@FL#3#jM)hEHn88q8vx}(eIF='
    'P+2lFR|cW1GRhAfuJ{TPGS&ogp3xl+<CEL@Y+P%`dPq(YJm_)j5%zxsX??NYo?ymf$K3H$M!$e>UfHpm!IPr1?m(7Nest1HU7Oe;0#X3uUNY+w7K&y?7u^jg'
    'qx`MA+U(y%ld6a26xf>_&Wa$_*gm>vg8P*nG_|>;t7%fUMC$t!pGO#j2{!(K1aS^-b*E2;bSs<8wEfyNp+5EQTg3TQi<L=&taOJh2A?*w>v!{1oFBG7t<ds;'
    '0()q){s5#mhucT}w=PYo?3kG)MOHTy-GAFq-TpmxQsi9TX@|@5yNI0aped2TD7Uj^6q-h@k18adqhD2aOxjHP>9~y2H?IxbzY6B&M%T}-x0xTN9KN7beg7($'
    'dYzNrIMr{!Yx6x`=d^W7ozl7B<w?;QxoNVZq!?C>$c=OM1uGI;cJom5(%R{KMHbQhtmC>iQ@ZwC1EY&sGMp(gCI-Er2V*rUXQuZ<qvhMD1D&lSeRh|RhLOww'
    '-%n<lml;Sm8qac-`@0h`fZhori>)f!79U|I-FpA5j2^+z!;E)}c>8y6Ch=^-`)<_iyx}HF(%a7s@#7oDP;B3LW32k%G-=bN70xtdHr}T$O(6cDkS#3M*^{N+'
    '$|SUn9CCsCqbdUlmdT67c$#;xzN_D?n%mV8N56pcCbh5jz{~ecO5Tx*dxPmh<F{us5_UUaa&IYZbxF7RiZ|<?j;`GdfjOf^zk4VMUHkAp+{PW9w?A>UWOla-'
    'CTEK1A&4^SzkPVHe9`3G?t1Tc7I*FT6%Ymzh*;wiE_#%GJ!^hpv`v`rdM~FjKaW0Yw)dLL6W4ptLz}HWNeW5Wd?%aj<TS}21#LTVTk84bL3820{$|I_ZsPje'
    'px$QRlqh^d_ay&LCxhLYaBQ4vHNeDzH1(ag3#$vV*ktD>O4kOpH~+M$(d!TQZfhqyYqIKw=TUr!h(J#cwtx19h0h`){0ZxMXus93)m%=B#_tiha(!>pq0Mfz'
    'Z-dh=y<gZ{Vz*;*+g9<#{zl}L?<8}WmN}J2!pLTY6II=~8Yk}d4;<9Rl?OmG)1<q6)Q@(<^BTx+v7ID4CU?N%1D+qy@~F}vqi&^V4F*7uK=I$Od~~=OHFG}8'
    '&{uT}FQebVi7j6=HB0<<w|6di<~2S-0Md^V5G@b~0%I+@)vL>!QPYyEmr`Yvzmw#1w^BZrr;*|i`mIjv8~5$1?3i%~{Z^l*+HB9Vf4eBYBLpxe09!K5uXupA'
    'Xzt+yh&oMx&pee^M^0`Q&&?9sL@auP-NTgG7t{+@hRiKS$o*+ZR~ckJHSFm<`)0@F%ow~U%V_ux@5%B_vpYyoa6bM9^Mvo2Xj%+%bGEBJIsJ`i2#;Ij+anVk'
    'a53-1_myfR>qjvk-&bRu*G@;f13F@Y6lw#%d{^(csL#wH`36-yU!t7%6?t?Y>pU?!8w>;wM%M3RZ&tzd6KFFiBbwmbZ{>GzN`2CBjsbvbu#xYa7tW{XmVxa-'
    'oi3$R>=BZ{SZ`cB=Y!)AWI2>|Giq+OpwaJ?dF~uXWCRoO*Lz7fo-0MEyG=Z&f43oa=94*d173b5Ud))9G13z{8;dik3Btq5lj<)s=qWX4eKAQUt@pa0D1D!}'
    'Z-YeN?chn12W2M(Ger-H)@ZET{`P{euz$An<OYX2zYD*E7%7pK_3O4fNY!9hxmh)`*?!{&BxmA`O(wlqMrQKw?T_*JCN6G(@oyj4ub!H-O>=cK`J+g>`SxLM'
    'Q1r0)_}i{x+jpaI(tg$hEs~q|`A5kIo~tH@_=4nvn^iP7fke-BlAfQd{08<CPAAcKe8q1-W}3p~kK!QhNqT<0;t4nixAYGLSabx6Nt5$OZKtu1ewP@!Om@`p'
    '8*pndW6+;vY4rs05EXaP0rT-4<z8?<B$Isv{l>mBx;KFQu+>dmo#5kob@|ViTmlrUCVc}G{k$rDtF~|G&UE=34@_1sUdkwa`+%<W=g3DE)6io3y+oc{nPGT_'
    'E~DS!q9V*6qVMo%y~X&Q57Ff--MsBGvVE;d?X=oBxo;F4o0hd7wmv5?YD;E+v@Fcz#D?^;r~7P>`dVmehTqJZIrwKI)mn)=Hrh87T_#H<LZ1ZCTkId6oL>;w'
    'DpuG@gEgezJ}Tc~<163fCr$Qx-=Lx~eves^%)nP!9KMgpmrMq^X!5ra87B8T82Cwdh`etpvf?NG)8G8+w?F;ycfbGLPk;USFTeWR|M9E8`-gx3kH7whzyA%H'
    '-~9U5_E&%VpX+DJd2@Zj+x|@3sJ~mgzxn+ifA{l$k5Aw{{>`uc`S*YLL%Of<(ffAIeiPrB``y2({(>A5`1zlI{`1d&{O!->XHo9+0k;2*iQnV#Ak~gk#&=_L'
    '>F!_t`lo-%UuY#6``4fT{9k|mYyK}%Q|Tk8&ui1b6j-v-+Omw2xl3cZSz&ErP4AK!W?eF8*n0Hho#zaSXcRN-Ruz-JIcBSvg%tbP6*JH&M&v5RWTaTghd5Cb'
    '3uniV-HMO`yEnyLYC(Oj*tW82k&-{LDpE0>RgprvSiFf851!PDWXgY`DU(P>bRL#9h3<jsTypX8DrFMQT%AjGWh9XFIz^2ab=n>TG@Tsg{dw0(pw$T(be&2g'
    'G(JxbETd?e%#2zmHW2DH=dC6a2k`q}As3LgYtG|}O8a9tO(h>JHA~L7B3WN;qdd?HN)s@1u2&khcOjRX5rCFz!F+BVq$qZ)aZ1;AB6s_`_tvdRR-UA`wUsP&'
    'OsQ6&_xVjyi6%y>$&adNoI3?6O*{8dP$n6WcT$PQo~O5|_~9jycFT<_68Y3BWEY9krd4San5&6pM;iI80x25#ob->1Mvet4jliel-mFLv--3%O728o&$+4G8'
    'B{0;=WRZBJO~}O)PSGYGCi~g7e>b;_&I;<gxrgH~yxXOUYqKO&c@Wi?f_PqcwKEkrfa?9y`c;0;uad1XDxU6Ha_d5>$roJO){o_9OqicqK1f(m>}EUYyHtFe'
    ')C(|W#<VNYI&LLYWwL$IRU=a=%qGQSPdoJ?6QdJ5EmQoow~I2_KBCw;>2b-KL#DV*<psCP#4OQIKYBcjWa6Ih-N?j!E9XWg0#DU!LV>E@C+J?5B?fT;L)>mN'
    'S*Mw12gBn@^|;U3>(ryXckh!d9u{pg$y_ir<FSWZrt;}pETi@=YPOppxjXni$tZ~GafLV^fiSvg(TfQEAt(Om+KjtBi0(c_W798cpK%y}dCiOyFm~qE7*1mN'
    'jb@T&oVNYP-(0A+e>RlX>FmNZM7Su^-R(pxQ?xHRcl*=|o*UOR>gL=rwcJ3a1Qt~i-O^~@=2e-1sP?krl)2(TRN>|4WZG<3{VW0?t{wPH7_3*ILo@L<OcY`4'
    '!<D;C$Eb2<CNjQ08WQ6CHXlE2Ru<i1oqEhk_EnCSL$Qs|?yR>Dq>=M}0c8~FZW$h5jp?AdO9KS+3(WxrT$e_g(z{+7u+>6y#&N3*$VeI6^FkVBY@bosC}Yu7'
    '+qQK+^{tpxCVu+HGJRSZ+cRN0Wo*Vq-juO6v|CW}m|^0em%hrK9aGv-&C^UwhRTIXK{A<_33$l~IBeG^P!y($Ay|l~EX6ms<@Kt|R9^F>$doICjJD1kK;@8I'
    'D|Y@s96yb`DAS#b(#e!M_E$2Il|~y^@^(~hHeN2AYXfjrrM-VQi}!~%#Lmh?_wq!h_=6bMWZE7>f1$D1D!M{G`Nh=?O6SkRa(&G~-j?CKTfjD|M>;7k_3C{V'
    'eIr9pMx^4rlocmnv(+?7hMi`CvD&EO%)&;rC}Q%|Z-3E9H0ryPH=<GRjWmICV;$793>Yp<EpV_XQ?Xs0TB$lm>Zv8%yy{5M`es?^GTDvgcP>-z*<?tQDF=Oa'
    'nF_V*d~SKiF1{vh{=d=(UgD1;E^1cVJOy1duKi2Ok=a@@Q@sJlGt1@+e9|RzY81hyh-y8vthusj(NeJGupM=k1S=nk;`vBgnK)Dn(2r!29(38rB>AwSoP#r8'
    'x0cDXI<q7ke+NjvxlT0iR;QNqZP_}}qGQ}F$DH3^Z}sV5@Hj%1f$k{=rR`d2lm-{1R0j`8Ak4d0@zX48GHtf&yOt$y>$#P0aI3i$Sl_Aeq%ZLy4Q|bb)t!ow'
    's-sJsTxwXwhj>(h?GKwvA8Va(@ffo|hkkd=P~XZR`u4^F<8;E2PBw>aoT8H*UBy`DP`&stPKx>MDSq08MQw`h`buM_YNa9P_jEOdOb9U2M*7C(jW#k007qxd'
    'RBt=*GA~X1FfS@2J*cn0Ob@1dy+IC_oS$7l!0Ho-ZZ~453oVbPH;Ce&$BmAwvkL~f5F^020W2-9-F9_xxi~}W2^6|}DnhBZ>Y+?F-vpx~lO5d?J8NB)%z|EI'
    'J{XtsT4+(GVx69H5bdij6VThx1Pr1|R0ZQ`Bohy1Ds_*_N!X>@=jtSknj20D90T!vk}j_(?e_h#7=2ijj2}prvV7{`fw<-d!Foe>#&e<@Eb}Z`4(tutsocLf'
    ';rg1&S#lTRmzKz`$#jn!?PL<BDtHjj0?;^^&L=luVrJPJ%gHJ--7%sYnS!-VEYq=Eh-Esr<SXsA>-$uo^kzF^nTWoXDWA`EZD^<s<q{*s35XLJsu+ty8!p6o'
    '(K0S-Lyxo(s<i>j-4u$a<C&wJy><20WU^;LyhA3ceM&2A=4G<7*8o2)Q?Xr@$s2T)yPZ-Snpld1cs`i85H4Ru=KM-Cjg}UStjQEiEmL%&=KZGl>9u|PX1jii'
    '=?(ivCX{C1c8Dg-?Avx!j<=0^eKsN2hqn~%wpU`~Yxhp}q-d{`3AxT&v0&TlIg5_{#APqoOlD^#<WxrGF5z`I!*a84r=;!+dsF`Xt;ket*P~3)zUK7?i0$4K'
    'R~^sXV!9jj^fb)mlns72!y60rZ1NYj@c8MqEqu3K4Kw)^5UD}|A;)N{*%Wwi#?4QU3hQngW%Y*Fds}Zv6dfj+$xumwXOTjiqi`1-kA6*96s<gznlJ5rw4f4A'
    'AhaH)2~a%=kMwi>E4I<k<(3wjd%z~t*HcD_Rndr3#J>D_mlPa-6nIfHw=t$P&Dd~#R~7xJ?jA2CCp^yUfzqdv&+BH7Q6)6T6b-4iEobZnuiWKiq~~SYty4pD'
    'T*Be|wv~z}T9?Yh)b3t-2PxfBj#1@U%<;5Rg?&?b;{{)VWi9-)Tc@7!l6_TZ&T>vTsBb<QU{a=_HcQw?Q8~PD{B_jF;%LSNUx8tHHif>cLMM~+O}c@19NOKT'
    '>cp_oqDxsrN_(bl)T?*J?6IyGiXMuY_Z2K=2WLg5&31j!d8p5L7NpJCw9W|5%%+^4-9c9mnRx3ksVzE*wl?P&lnXKm`X!lm+tr9cw2wk_jxkzfGfphTkxZ;|'
    '^tMr}o?|4QL~56c`GV&eKW$cJ;vT)%%apygV{bCu)`Zn5W^jz*9BS6?H}_E`{3KwS9X!|zA_Z#~HIi}WO0tgYsU)U7>%|i>&3NaW4gwlxyo`dLNGF6%`B|zV'
    'dnSn`{@|iua7QKY1S4>DN}UaDg<j2a7&uQk$sd%=)iR1fb)LkUI?&>XE(FWtr`LkzyX^y!r1lCNdgLyoX?W&NAt(7mP@JcIR9kw^0@p>2=;qVL;-`rhWwIMN'
    'T9YZ-XX`pf-<ELz4`NWV)l;j|lu>O4IjL`@GA-ajC9ES^l%`lWkVe?l(&U@_Y6hj`hM9<_<X*c<JI-xXza`J*(v?i!8#U`9J6DE`ut<sx^`a$k;i721Pr2<x'
    '^Oozq$wQZhC?I}PG|s^<ZEltBmWnejjJ9&d9Vc)1na!O2@-!=tp-;1T7_O&T4p`UQIn?|4P_Ur(Iy^?TS;nCb;ib|7f29zC{h~^t?}^x}6zaUq=%jb0K<SQn'
    '?yVP30Av(TY9G~{1mUJ$!{CLgJcEmy`DUB$NlBAUm+<awwFU(5N6P30i)9p0%`p%Obq-zJrY;l=$4}c;Z5|XXM*E<?pNbQv^)fk7nA@0=5Tx8Flu@T1e<kF`'
    '?T1WorIN+N+Ks|)y9{Ki4=m1cSINT8j-<&+(dR`zVY?(pZ3~KT)4KgiRA!J~7}AAfAeW#Ta<?BLF23is2T?12;`UgF3F6xBmYM_9ms;2ocFyNexYZjen@}*$'
    'L*qEnCC3St-e|k+>KuypQPV>N+qMrbPBA}|c^Pg@W2?wyb01e_J_TsXHl5_GqG_3Sz{1lq-SL<^nV`9DaSWE)-P%g!wAhhMz7M!JGL=}4Hk;7-rUa3+*_5wa'
    'Qn{H+k&|?PaJTMgd2ac>R;d#WZGHAl4ToBHDb>Nu&8-v(M8&_{N0l4TP<j#^1=SYjyZCsdrS3F`b-GNJLue|erE`~}UpwP2<pS1@qT9%XeFkJBlXIr2J9iiI'
    'p5mw1@}73v^(E-6)LtbbwYxa+a2E&EZ-dLISG)Mc7Z{q_l?1uaVa0&wMQw0=8yuW8w@*R1ZG-`Fsm&Zr!Y%#mv@@Q`UV&4+g5Zs<ck!U!KGBbLGa*CH--S~a'
    '@1nv<5@0V{2=$&GJa)l3=9zVe=Hzy;5zU`9&DhP5e7SRUvYUVL`8D@oH(Wo;MdM7<xh^mBy2DC5bWg;4(^v7?CS8x(yJ@$Z5%QDP&3nAwx(;2cRI;9+H1_K|'
    '`S?Z6%7f_hDp+rtPCjjdrY~NwDpW2qQh+MAQPA3FMNg={Cy&1znfEv2r~TrD+KktmK}7>yvyJDktY=jDFI07~$DK9mU=N9C)j`v|;{f==Y6DoRgF1|tfjaSE'
    'Rl7_c`o4$tzNv<LF;|MF>e^GGZll_%B?=X=KDKzqD-i-VD=)mm_<>Lupov}Dw#?4Oy&ESS^7LJN7HsQAy(W}%O{%Y4Dc5kaR<3cFr?(YL2G;p>Vgk3Wfnyw~'
    'lj^jCvTff+wO1P$hTxhA3SWw#IJ>6KW<2ZaFi71DB%Xh}NRwrnn?}jaS2CfuQN1WX+JUb2oUEuBVd)(ljA0WH4H<h^fw0uf_IMB)h=xtfz&hztH0Lf`w(k;k'
    '?WNH$SbIz&jc8ZGn58Mis!%dqdrZELW5%N-wihfGd8AH$U8w3{PugzPiBq|rQf%~4fx%RBAJ4`4Jbrf-wU&BuJjg()a$$FD=Z@QT7-zhlnNpZ9n~n0`yN#-y'
    '{ET>0-*Ylw+0NOoZRfl3gZG}~E5#!aMO^}2Fm)C=j)bC~t<nfZ{c*e%D!Jr!!=9k;SP~sxu+bT3jQ<h*UHhJ}bUr>FWguG!O~;k4Q*zu3^iIiGYDYCN)2*^p'
    '%PIgs=z1HmDivERf#b;(gna2g*L6B_t5TIm(U&5ikvhf4bv<o!vgAaPG$t2Ly}dg|S(DA?dirB`hYMvdE+y=*3ALFw5UTA_?n)EhI%@?1XW|E#BXgiG#S*vE'
    'a#^DtNYFG~vxA*J9Wb_ka{#SxzqZsO>Ub0x$rftaklRpta;Rly2cuhF(W!4*M)huf@i3P|&d2;hr{`)&hzE|iX^3|v_ph66xYPsjcoZJUmOEJ<vyJ<jJ!5&I'
    'V-4m)+;m2Fifm<63oyh*Sr^85Yc5=h#!oL@iXP_a?SZ3#O&i4MB6k4>K^du&$pq)14ca<ss^OP-ny9lky&zw>YY<$*DjddpT^(_*X~s>h@6NzDwQ13Id45jl'
    '%A+Ug`A6gQIBn{l$zHC+J-w{1x6OF{kdunl+NR02jeH7=ejK!0C&=?Hy^M<PhGKlz)$z{KZcuX>Od#|r-9DA-;N6l7ZpnM!A4T8Nqq%6CC~+G|_j<Ps%+<<i'
    'pSFrplux6oQixnf^h(9T^gP0)G@rxx!92qJO5L?7-d{bBQfGz7S2O9myp9L8x6DNCu#6O_UU240y?*@kQoa6RJPuF_;zN_A;cb`RN;6qw>AOPiG~pGYGF3$l'
    'ea1Z)eRB`W?wNGNbb-3M@XB*IT{CC7xoQyChKr-i5@vjLn}Zpo+&Yv|r(Vwhrq&!yMxvvMiqN=&re>Qdk=f)_%05$Z=}!50C^HZ+G?NbMys{1^K;F#wS8f=2'
    'cel10+G!Cvw>>$we&i`B<0(Mg_9x+8wui;FSgLy*l8=@*ZvhH@2Y6RM3XK$G-L-JaP2zzYF&Qkdw=?IUH@#tw$6FI$w!%>&=$qcepIz!ic|3~#f{6ZUy@r!T'
    'ZFlqF>$`h9V#Mhkn72{ZJu-68yh$zj3m%yzyX2Aihb@?sflV5pHC_EfD!QF0&^oB}Zb+Kf-lW7x%@u2Y!B1bYqLD?Ritz)XI47J`SFEACfG$U(ire@1{|3Jp'
    'eJ^A5@mckMH99DgjEk<J6p_Dhegv|sBHdM&-*v8TuBXX&{s#If-qjb#@C#)41v31;Aj7N#h`Q#sh>fOGz&eT6m<#Na>`5=_QViD{nn|0<*G2rIV>d;Z`$9cl'
    'MLllpa8mRNxv{=_F-BA$wBuy9Gi`)yJZ^^V&s;Uo1yO+(sgL(u_CAQ(+>rU`u{X<*jbl4;RQy6^exWkIP?=w-%r8{t7b^1$mHCCryo|~eg=bNZIyG@5x({^?'
    'FH3gEW+$1HM6LyyHrt&Zkik&J``G)oJ%w0tev*#)MHKxaihdDAzlfq=MA0vz=oeA+izxa<6#XKKei22#J5dyKnY`$tv3gpGZg<^*Fp5U1Xb9C+&0Tn%TuJm;'
    ')QktM1(LCKqj}fe`jN}=)**V;=t5Ed8*2fXRF^QNwDt?Vt<kM{OS|p*U7x|a-HAnHtM2F)?H8f;i%|PTsQpkvtv~CUQ;jBYkR}s0-FC_O`E81vU!6+X^1)O>'
    'syUg-ziLI|ATn`B`i1fR!uWn+e7`WhpN8@EXVu;NDCdG^HJ6O)D^R3ho@&02i*LI9DfBm8AgKI-xTQN$Tut<*#)^%k;~Xf^r90_9a2RCkjumLGbgW2y0aqEw'
    '#kzrDO^-b^T`oGNOsW|`R{km#_Oy?xGD+Q32UsouRiK0;7G)a3o}zuGT7AH|$7{uh4rag>{eoyuK(zB$VNl#Ayw%}q5N%`YTs$<vsx23sNZL((s@E#7{&vv?'
    'T-U5?%Go6^8e&&3IyTyYIJ#FLGl#w}2Kg6*{EI>URtEW?XUsM#1IdtmO%%HVX-#}DzbMm#CNc3>TKAico_fd10&iyS&kOF{D0LuZ6b8Bb(iDO?69zr5ByOxO'
    'TI$no=KG`{XGCNf+-}@3Nu8SPebZ@X(M(0BzVP;6c>6EB{TJT;3vd60xBtT1f8p)F@b+JL`!Brx2jJ~d=O6i7S(=8o2ewU$D~k>|h^ORmYxctT-FlOli+FMz'
    '(~#mJs=<w97;oJ)8>h<D(l<FhO1zJ1nvt(j2hN!b5cl}$Wr({QM~RtQl~Nmm)(wrP=t-GMTLx5Qx@+3d$P`a_HBg%6!c|uM^x9R{Zk-x(N%l3^ZRk208wQw^'
    'DX0z7_E9arjN`APj#8yAw-;oB<=GU*{n4U*O}?<t)R>0cUWbiG8&yjCH%hW+8TD$iCzl3$Rd$)V5Sha46t2j$*{&}--!uubAZ=xubw+S*HbIV&pSz)DR7Xd0'
    'nzOY`IdVBI6WYZ^S8Uha&}g4&z4bO#$~vm6v1tUmhqD)3JsQ+>1ZG=r;Wl^Sp3bT!T7=yf%bhM#w&zHy?%)vCN0((%q=NNhMI?U)nB4J|CI?%OI0DL*U@)$E'
    '1H0{N#4g%Lp@}0H?anW5Qivm&?r0{BOvBDeVtk#lVVSSQ%z2G*aM-T1qmAe0J=cYQ4c4s({YWLz3mNuNjd35>eeDeddqJdN?V?69E@Md6)xGCMOdD%~ATziz'
    'D7EpL%fwrY50!Qw7hJt91-MYYXcH@RQKm3<nWBAIPbJ|lh+x>xv`{Z!rL}k4M)gjM@o;7unJ)Cfiy!vOA`SbwMUm=;ZgDQ5RypKMGp$B?<*Xyccgmxn7wZXO'
    '(`za<jCu-m@516b+@3=9qJ*F(x8|M*bvv#m;cLWi-`d)Pg1Ltg-$$&e<L8d(!u5Lm^xF0MZu>wasShp=J#u$bWq4*4N<MH0#fREQwf9~D7gU@dywKZ?%~BJR'
    '!#bV59qnsi?hZQ$QXoHbSDf2YKv4GdeN=n#xhCB_wKKZ_iwe2H#ga_6r%`k=Mf+;LgLKVJfz42w+=x?^ri^L}%{8S}PV(*LJfoasQJP}iK$@nS%9VWXnt3$G'
    '8{580=~s4)sxv4woE6SJujdyU*mapkZOL*&lX{`~_68>9ANZLFf#k$mqlC}p)@fcj)8$T^HS^CKovV_f35JZM49|?p=m>yr?Gki<5;bNn-g3i{gsyN_K>Xy~'
    ';(SA<EkVg83rh4UdT_Re9dQcpJmVU$UtW#nG4$2Qrdf^oR=ai%p!N=if`#lY8AY&ghUGl02=zT*YKGyjw9Z(90Poi6x5ml7y4@YPuE$TxQJnLkz^D|hV^niF'
    'g_{l|1~0VRGPwB8+icT!ENSZX;y&Jnemw;5N6P4($jT__xO<UMpYh^0bp;;jy<OGjK_{|kA2e<g^<>gevkGV|6DTN}ZB!3rN-%&8!4)qNT#a3p2}bOhC{rC('
    'agMv*E9~q@8a(THdn3q~OggR5IyoJ6d-JHwpf)e0tfWBEzD)n__9Mi__uTd%x)&aN(>&i`x6~Y<zSP2&Fn&IV!mUG&vIzx~>mw@vk}mm|OOX4+c6AO#`x>6D'
    'AaqPkt{o>B$z=6A;II)GG{-4G(`d{|&T5~QDX-Hdne6!1H^sG@7eBP%J`YxS0#pHE>pyxP=MA@jvo@R1`KD<WkqmM&QiCLy!n-NS;J#u0^4#)$cb!f&v~?sU'
    '^{Q{(C2|Jm=GKOgRz%xIl~7$M%|?$-i54z!1(22+n;+KcG9`9l(-KiScPaX{GwxDuxz?52G%{gdz|_d(oM{?Lx(mtC@zZO`(Yx*X5_C;{p2v*vtYzZiE)J-t'
    'jFnNZwl|M2Fs!jB@vU>Ab+Uma#_wS}>e@(?ZV`mr4!1yVs@f&oa@0?`qGr1`)VzY=O@rMIYPtpe=$nLpcZU@Xi^&(^q*9<4Ewr8UJ$9j9=9zVe<|G}a5zU`<'
    '!`;o0oXv2MVlI$}*4%^LaQ!G3jWZ1yCNJ~4b#gp(Ph3i+uR0}H)uUF0tR>*Kv|YLN6fuV`^+K_ppn!Aw7TtbPv+^MNyb9JgU^)XGf{4s$GUYZV3XSeIszO+!'
    'C&Ue)DaaK-Q}BxuYBOH%@*WLz%{HFDvYt`pzfje|954-vIynVd#iJ#6qkhK$@P*X|u+&84Fn+*lPh|(S%jBU)=G6P98w;h?5Fevn6$+Y7DXyd|LZM$5YFIa%'
    '4Xpd%XDUUF@sNt-JJ^?CNG!aKs=!Qxp=MS&&v+%0!e$A<3Xj2r(LiWoytZw4<l;LHX9#<_YZock<(NA!rDP|KXm64W+mzm+o)~R{<O_$yCvx{?c*~-d2N_H*'
    ';ySCxrRG`P?dDYRaQd#oLnB2r&Z!vcb`>ACE;i$Q7E!JfkR6(o@9H_8Rqy;M&V}|%vXw{C(G;d!GuF{@t>uUl_OsKD&tl=-sJf3i*OcqYm1_QI<We>NVctkH'
    '2X#$MocrxY299x{PHOxa6tC(wszQhiLvT%ilP?80*RZO)@vN)EAa!?XCw(B;)>u_L77CSW#%)yZ@*3?xS08MwsGC=Ek%KX80%GY#*==9qmPNx+X=5_B3F%oU'
    'U76L~mGSmnqOQF(I`Scy>V`k@$*<fp*tLc~hw+28$K>m4hPb<S*%=mjq)ztcRCRK1S(U|P;#8*QMqQ8O8B9$#EtGJXyT4xdi%OLT87NiK4yrsBZr5R)@iW=L'
    'h&i*!DDKv6R3+#b@pXUK=)7Fn&e^YR=ezL(cT4h(|M3c>uAn2Bx)KyeLQ$526rswfy0ZswP176SUg?cbxGGdJZ!n{Ruba5VncdB(_+GTrLZx1}K^5wx+goKv'
    's9WLM6tmLldt|`IqYz{KkKpeb=ZB>@-|;Ae`4vLb>%4=7#!XX)g{J>~cQ$M4QP7cl&?cEX2v@GMAgslzcH`{;P3XC)F9s93`eH{DHK2&21~z!>1ZGsWBSME?'
    'V0BKhwGueEs1Esp$mw7VZgoyQ=w}nnZIF)Eb#%g94mKxSy0NyZY}=?;k(kn?2-_>YgX2kQsiWdy-asglnw;ZE`qmogCa$x}SCJX%L`Kbdg}4QA<5z)P(Fy#z'
    'PQ%QWY+#+vS+J9}PzX2^SjT*21?t{jF0{0&#%KpNNsEMb;A5r8+xBn1S?WnTxVU|7k0K-4LM>Z-BT~10`I*WFq+9y->8Y@c>R={_Zl<YaD>>H6_9ek!h;Jdu'
    '?DIR>EH2f99FM{S*)o*um~Gs5yfKz1`UYa|kd}hiPLZvQs;@_ux~)X?zb<48#ZNC~3mxVSUXLsd(JCHJyU4qsjMT}r_tQGQGMa*FsyZTd?Ufhg3zzVMOQ`z8'
    'c(1D?&NcYiZg*#3oZ7S~-+{0aYM|Tpsl5f!*qUk;U;kGwa0(rEMX1eqJ-h&=P~o@|Y#n#s^oAcC1?{a7=c!J1%d6557SYG@TrW3wrGD1CEb{X@?Z)dmMFZ>T'
    'LP|1gg-XBLnSIG_9JKSdU>Ozt%#6`JsP9e-Y)kV-O(68CRz39w!Mj~5IKM0#j&d54xoDdk=k`+G>y2|Uw?|8_Hf>5#4xOn=A#!~?SjzpU0lm1CGJF_6m{OR%'
    'kYh^umWz^bNq**D!fu>tbxMsH*+{PLa^vX=W+!ER*+rp>@q<Z3j+<9q+t|ii&of1Q<*T%|8r!H|Ta>WxRO@7}G*XP8UTUOx7>@&;#D^v`*4wV~ifB@(kh{xz'
    'MW~SbyJ^>`@%4_r$vtEDObJLzT3g+q%5ykfGiSL;?-18s(MNAM%p8|(j!=`5=}|_Vni2!(O*6To9W>FZD|)F3$(L&EOfFyPt3zmUac&P~1_Fj=(m|ce-;vqL'
    'o0(Un8{Xo(Tfb8+V1t}n9Ureq<S8jr6Dqg;NqCoGYjN#@%ssx|kCr!YOA0+s+trUkBL%IB>e)uaU^0r-30eYhAuums)&ldp@%rHhg&UxDaBaefaEgOX1aDlv'
    '(^a}Qm(f8kGLXqQ`<)KWLGK98>`+Yf-h|^^g1#eo{MjWC+wmy6#}Utn^=6kWYGKjA*NLTeAlvyBYd5N`(D(wc`@lrU`3h890xiz3%{+ZmO9s~SOMIL3Cn;rk'
    '=B-dV&{<_6`mH&#>chs@wXgI4MD0&ZY2Eq#Z@%l=7=L!D)#dRh`m&7X);zY9{MYPXY@FVwDt&=YN!~_TzxyEvjlXZnUr2JZ`ATg`od=lYWMEUcWKCBXpZe-e'
    '^vy}%C1-EIdX#lPmKdo?Dn-1KREkCxg(}7mgyNiHUnP}>F6Fkwx^C+H)8G8+@Ba<D-~9U5_#5}Dzx~e**WPUxVW(nkqh8&rq6b<_<;W;u3H*`j9C)}RKKODG'
    '^XL3o_XOmAe-Kr;c+PLKyQM|7#>2ME-2CJw*Xm2Lj7Fy#K?D8OO*~TCy)IdZCxv)wiG~Kg<Fdscb*qZW_>I_JZKQ(lRxu<Qsg#dFr*ZjhR0im=I)IuoBQLZ%'
    'iEqGFVT$QG+BQH_N7&s=$0Lz`Go5dCbKM)BE0cnfdi%=g<JR{T!Gk(~EL2x?a94BU!lFvsW9W<(Y%mznYj$0>pUy_kWhD89c6P|#Mn&D2e2(26t4u?V#zmGc'
    '3Aeu(?$+2XxBEGb_J>eU*gjZlmRMI8&pgvBE)`NjCXt(SwIH|Q)=HUDL34dy;d4J*MXv<N2)5wL?}Mg3^R8DED-WVq>7+nYT~pz^8+^~BX~j3@)R}9(kE)4m'
    '<#u-$iZ3Mj#Sbqjvs*s!%e`%Gby|$3m%uZvJ`Ha%$J0RIHaZBH#`ytl`4U~IrpQ?>ODYxH^*CU;HmWL?s4LVhfCF8epdMMaQLo?Rv}vNz<v@M=Hps_ca97+_'
    '9GE4k!h;hFlWT6jq644>ue@`1q6Q@==%~#+w$Y$p@Ptf{J{F+4kT&CexTVcJr`XMQy_rWM=+;Xx)#<it&=I)Dk(b;*^Q@4v9@W{A7=KZfm&uspFtZT^%o;Dr'
    'RBYE>@wn*fNDE-wyqUAJ@HqR!lrBb^FI7fG4IspW9&;Uu7kJ#KG5b}<#|MZOvZK3`%q@53Y4NxZly~A$PM@es=({jzQzoF_V;E8J+V!YY9Z|GYcST_#@)x31'
    'C8cyhrp<PzANQ~cf_mRJ{1QxUCnsKqrT2r<zv>uOf}qhmX@YqaMD;~2r8hWA;w(F)K0h-4?ApS-*{VBN(b~E%FJoxS#G?=6rWR(%$6!$o$zjW&(H179#-o*J'
    'hOR_F4G}KNBz-AJD^mi%)RDZ9>#XbinRsBIi6w-saO2z^y^U(wkp$qH3f};v&MmZ7*u~>UFfNl9`n#||YocvWqPnNi+9oB-gtl#AbT7QI6RkhYtqsv>H>Z76'
    '6OA%StV)!;(&&oNtSrI9I(6Sl_DREB=^9-{FAUrRapZ2>u0~PsT%5b;G^G?*YEA$Hu8Sj0?5r0jPu-)E!nK`v$qZ;mW)wj$qg@NhMjDG<qPv6nF4)c!EK2hr'
    'fKC<~Zb(Uesx#Zs%f!~5-}+|<ymnOc(8;>mEl43mxq`1-@}3Uc^%)d}sf$V=o=-K10i|4xImNu*sXvc`j!h;9P-C=8G2k>DLvEQ^Pb=ofz4}w*s#-C!ts@ox'
    '-!RqLDmTmkP`ZSht&gMX6R`Lyrbq0oJd-vPe-Oi(V%uZrZ#tH`Cn$N}<hD~__I9EXc5Zoqa2HLeNA(_8@oVa{a&(~#Jr~uBGgMZbgUwXa1Ry)f0@po_;~c|Q'
    'GjD`udaT0+{X<YRquxtt?j4M+E98iQ;lj)U2a7Tl+tryB?XyzPENPP><IJlQvScJv%3e}>s+LjEFR={!YLh1)w2Djo5mS3v%AFOFz4Scp__K(MqU9&igEH1M'
    '-DeA}t=7!fo%!&AX4*DiMx7dyu}134QsOHWU&OQ&x;kt}T_vfr6DS^|nrmfBfT4-=a_)DlW>@W_+FgVt#AciKjQW`-9Dmc@%wq3WXO@gJt)j8eEXT}pu0fnR'
    'z@!S5JzjOII*-om-)*VxB1)xdeRe)cE~{J7X0u)2sw~wwW#W9(UB3mlWYTkSa(%EtrN3ht)jQEfXBVpt3Nffb7Zjf^aLXUIkFw+9k!F7k{gR!bzR!xYzrD7>'
    'xXFi!ywLPjHb-}yqL>|BZCU2J;28xj&?DogU09T<*sd=%=DLO+IlrhYnL-IwsoMCn()fLpbCI)}jX+d|43L6(isyrioA~||`X(Nz?n;D2PU}j94q>W)|E&c4'
    'yo>KXs_o|?nCiw0{Dt~mDDJ|FUeb4g?ez+#(hIzE8qyB9_zeS*v=($_G{NVUbOmRqt_DXFv?E~LuK+?i>-%&d!n;6T{8?NVZP<@4TFce7U`nmG+9U%j`fWVW'
    '?N0l2lc!tVnc{^y!Ww6T;Yz<N47|3p@3yNudx}QYTUO|9nyJ9pb!}{}&Ok+*GOE;|I%{2ZwS`_TFty^RJuPZetkZLYqJ7m3Ajb1>6RkpKse-|FBohy1;=J&*'
    'Y$^}N*Z5ZLxR-AI1k;^TFsc0VuKL@e4~vrV1IbbXUPHWfEv3}5OwqXMq+9SIncZjzS^<d@ZZKq)F0{^$pC-E|(`LI`+#KyAY8qES*AXnWIiHwWc1u4$_dxBf'
    '&PJPA!P+L0ex)4F!CnscoJ_mz`sNg<p|u(8pygIXM%vI&8_L=kO`BY~vulHD-hR3hHNktEA`EeM+y0Ojf0l7kDSD)oP^}c$uIGXcj%VVZcFT(}nrQYch`eVC'
    '8lr{{%;o~CRJ^RpRBTse@+Mb+RdirS9rxB*_qdyH`6@CBFUsZ!zn(elO{f6~HhP~IfZV~X?cz7v^&56?*f%nvbmUu8X9&?qCdmM=pvWY-3RRm;$aVA}UA)*U'
    '*EDOlpu6pAv7awVconz2y)4e?bTcJWX_Jl0Yyv#%9<!Tax!J{2g~t3Yj>h;knTqXtXfx&dG+n^30|Zm3OV;#Ql*^1(4H=P(GmVaFi5^kiLE9F`R@u<K^N9gy'
    '6O7l~p<3e4u5IZ1lc<Me?;5@h1>d&9w9x__DMfu-hej#NwFAnl&wFYvWuh}B6DeoJ$x($6--B@*mAhzOZsEM7S$QV4;vw2?YRx3KZ>l4m5a;XMM05T7wvn%l'
    'D#x1CM7*A8CU^N!G?N6A+5Wb@X7NXX7d3M|+^EwmwH2xMu;^m77pIs~Q6?^HN^PNroAS5CsT`9TG9UCvcwzB+L>=Tv5)X8j{=$A~N86u7bt9s+4c#S^v}^A@'
    'xG7Ahzi~=H(X5Or!F48IlO>!p_QD~@<&4ZFmF^fTol4ZIWPRto$FpLXIScx&)A{-Fc^_4lBc~SZ)pBIH5HyaTUJDxU)~QEkF4cDX3S?+I)Y{<fl(Xe?Ntn_b'
    'Wgpd2WjP^wYnkA}Y>J;=o=sug8YSA-qz?HE>1jHxil1@aeNF;2N`}X6RP#yWJI>cWDLUf^wUKcNr2a{iDopq;8k7CS716dQ(U+ia>Xj`>o5OA`n5ld#pS?V*'
    'c&KGm=XNp!m|C<Phnk&R$%|f;sf1`%4>j6nT1URUeYY0Plq1hKT;OTc>NcwKaZO(O$LSgMmms>hYXG2UZ(L3~>dw-*_7dEmM2%rZYilM)JWCtxTEa>gkl^?P'
    '%Fu|$nV89%XiXDj%Y5PVlTeJSG8Nl(LbdVSZ11Mq-3ejlCCK@nNK5S4M>Sl0oZ$6KFxU$s1#1^Il5zH4vaW7;EMoe21I|>7#IJ(WZXDI<X#Ej3{f<-l@l)LM'
    '+>C8Su<a?-eO(A@kY}#vsxb*e%<xu~3<~KSMhpo_^(eKUP8Y6h;-}ZHY<AlR!bt5*IrP{jU#A4anLH}Vhk&5u@9(49rc_$=RxAm;&>MiwQqkmLoep4*_BA|j'
    'haG4GqeJ(&QWA0*UFwmDicDox+m#BR=x7vx?gEe`<bu2<nQY&8xsxf{S9da&uG@=A9iK+pl*S~QHf2;>X-?{k<G7~XB~CTTqBh04fi_Y%qH-m*tL`sBB9kl4'
    'XD?P1nSAcLr|sF^)t%zvA;zdtJt9iQVi29;xC`6|4!m2tf7qWyjZlfV+`!+UO;dO5#JV}rI0vV-xg|L*PdV!e=f>;D_LL4&&%Ef^FHf`b82U70(@e8`p;XT@'
    'n4;ySZwmR{Zxq488Px2sBGe)FREXv;^a~L7iz;R7^b4D0U)^s4Tvu+3>_(e-JV$EygJV=%fs&Knjy@)Mq1xTx;ugNyrn{EX<W;*Mw(DNf06a%44)mS?Wfb%}'
    '2a!-GR=d>5<pS8v+f|t!^aP0ZLE{k69oKEpv1EIEr6cw>s)q+87{G>lhnJ{#YxjJ+?J^LjKEgT2UA1F$*kCA5u8=u*CBv4=qUD+=G#obIS3AlzPkcU3!CeFH'
    'zJ3;Q@ddX%iSAei-!#ZG*e$md5F5bGOETU0G`fvyXOmzYH4V9(hC@mT)##0+4ooik1NU?h*+JgVDYZX|I;oPiO@A(g4osGNpX*9l{kA1+jTViDg$Gw(tUJj`'
    'f78BYUad>o*zx^JfadZub@Zjy@dmLo#c5&d19To*%A=rH_{ZZ&eLP983K?8}5;`N28fd~DHY)z?@`UnzNoyw>+BznX4kztt1-;N%DXrn#Hmau}1oXD)KaVhI'
    'W|n&(GaDP=zQ-xSTJU6lkbzLn*Fqs<SL2_rzg3~O*0YLG+o&c~X^3=mbFpv%l7Y0;4)3r|*Cvs9n!YV<=1=_fa1|H`BblJj&~9YHKIy%YDZas|X+M7<e=~lX'
    '7N^i=yS_m=E49Ck8Bo>s=Nf8*dLBy|^>>cu@!bLIhNh{H@dbw)JTGd4<II(i&{K`a&21Mh6Y7itKb9$_(VkMXO!k}fbf4f&lfN0%Yv1S%V0{F*!?}jVJK%7V'
    '%iL=gLjA%EzHMpajU}4tyKhcz<r~e)sHT}XQ@`+>9!iND+3lZzcjQgK=05F)>L=c0YSSq<ulc&yTRalafHI8IAif^eJ12$sCAMuOIJge>oRD#d2@n`gx(fP5'
    '&B}x5vn*KOfHF**b?&KPSu~+?fv(ap{x+)e#-h{6P0Q+V1)~+UTbxjv@p@y*XrOEM^!$}g@8vn#pV{!u=j;o!<x8qSf^>`8G)w}%&;%V|sh;>Set^79P{!KP'
    '^j*)~=pa}(g)<*PLT;lfqsF9IZXGsYyb@Dkv+^Q5j2{Sv0h(y9ZL8Ij7aS&TSupopAf?eF=CXM+no;Fatg41VJt5KikYWdD0-$&Ir7_E@l_$~rc^cw6uOJ@c'
    '+hhWlx=V-0hrje)d=}L0MztRMChgVrA^A$>o|Co8y~Df#u{#=A=T%GTD^)oS4u|rjE-^uQ@!dvspn3cyxF%@Fmx6ZAuF10*uVY9}3We2CJLy82KGe8BKNbp='
    'l9*$ZRiUuCGPk0a%{pKvlQRC|w8y)}w?rQ}ETw^F*aW4l>xjznT=rl_bv+4tp~7InwS9-<<6eo8myT4c>d0`xG1W+!RJVX#9C%PjoW3EyT2*<9U!X2H!MX)k'
    '`Z9eJ2k(vH+(j|B4(t;Q{GwFlK?YKl>)<NEpoNmUL_C{;l44HzFe-7njH<1ejd;_D##c6R_G_E?Zv4PUkbGf%ggU7Wn5TPxo@1DAU86LisPD?q3Kg0QDx1#B'
    '`a#CT&rAL@+!5>Ik1mB6jwczY7DChRtz%o*IJX9GzS435RNQ-M$e`o6v1$Bj3-nWw3M(X?+!qq1g|Vnrd6WVC9gWn<b*^I(SntG7!rwu?CO$@4g}>)^KF9VB'
    '7m6Me03)mkwV8(_+&eei^2|}FUw+p5(X85l^2!1%qw3o)&X5N3PH0E_JIU?e+@ZE_0K3$o@^};(i4|(GklQPosFgJnnJfoTUKEUZ9@R1CHZ*Q%Nxai;9-?<+'
    'JiFvNo4f7^ZtE)ev~2sM@IbcQTKkw|+|_BPih-Fx-;S4eifm<6m5s&L01Ruc0E4Yrx^h3v(@%lXz@`y16<$~sjiCYDaL3EDqE5l9X)i-4FXe;ThdKbuV)D!q'
    '-%N7z+B;?+E)lSfNAbE=(MM>QD%5)IY;q07%%m!b8s{j@JbF@JbU<(7B&vg<UM_S*kDp#$eVg$ETz4w}Y{(b34H`>EQY>KK{eWo;9n)6DzNc2Fl3;W3BQ`oR'
    'Cs>so`~I9(FTlRLa;@{x-1n!&S?0cTs%@O0uzm>l{b`|=x$mjn+>5yHR!3r`Z!uHccT8QKOgwS<)G(G1bqM@|XsZx-9Dt)gkNp0$P|M_ZOGfOA$nSQrwWnaq'
    'Rq}h><!u1sKwwhC_xoM`K)*k$;7if(n7ABs<njsjyXu-^bWP<d`#p+j8g?t|?Dyxyd<pyA)DdU5f;1mw_a&rxJsk%~^E3z1hxp122yr^zb)cw_=1<GEjx;yP'
    'w#R|`8B({?PvAtM?yEi^b*B?JTfF`42B7ZeRC@ua+ehCKKONNlv^wjc?sz7eD7*a>Q1{b9ErYr#H*Y!tb<@+JZdO6vI0fpaAE{2*I>|oTIo<RwPIvHAq{$gh'
    'chE@?wsAXjPIpu+ol(GB-~i^XIa9^yrZ;iAsVVUepUv#1_b|J+_KFM4?m^2`*Id5p%x=mJ!75kF>(p*~6SW%&SE=2gQoH>WwL8jGqs<Me*lv0cwi~f_ncWh$'
    'dk|?xOLN$6`VoDF`0@9p0SC96-p1_?HC-4#8sSZELU;#PYh*th;Z1Ktcn9!qZYn*5dFxH$;#(KzVBVCdW7XVdo#jm*!188vb`Bu?2`q1V7t1?<m0u3ao8HCp'
    'W~}?5`4yITLI!I%>eMy)D$6_hTXAo*amPyRCa<Bq>0K!Ak|+Do{BC*|zuTtV>xp;aB)>c8{BA$R@21?`E<i}!P!1VjsPVh$UHoo#Q=J??gx^i?;&-RsRx>e%'
    'hy3pR22^D#RnHUr?(^0sf!1VNCU!gHd8X;}XAryTZNzTt&8HK)>0QKbsT&pJhr_$+ZSd}Z<USnPO`m}5_9NcdXMnotU7+reySDsN=-l)kI=4+(tagHnI7#Ow'
    'ozCs2=-dy3cNLzS-iGJ4mdqR+_yh3V^e#NN04yQ7QO!(9NN$-za?_8|&tyq8PwD^O5hN=jH@%C<Eo>)e(+3c_>1{-A4?GcBnIdurHL{X2^}-a9JDz||IQ}l('
    'h7=?>y$i_=DJ5ot*7g~6Zh9M?+eww)C##%puXDQX45vH31kuF<DCa+XN_AB@-Sjq2_XC+e0M1Qsf^$<&`JJ0hAI0gWPvCU3gE{>B+cDkr37GC6w1nx_!<x=y'
    '4`Bk+9epfJji$DL29R$01V}d<{&Yw;y$jNfwx4UC%<HCi@wx>p&$JDl<aJw}*DW)=Zpv-cT%2vGOabm1&mFjKdKa!cK1)Ti^B<1urg!1GQ{BWwV*iKWy6IiG'
    '?&uOvVSFaY-GgecIH#M&O**;zl!v>g?$f(i-7dH(B-CfHy6J7K?)ds~vogJ%)lF|>bxWOR#vjh=rnj-Wt><43tDD}%>W;hPOw+VURyXLZZkc0sQ}j`s2$I8K'
    'A>(v3R5!f~)h&ZctXWjI(^1{wDylpBoz^5swYRBzv~fDBo8E@%j<(J88J|IQ>zzL0lCQDT15|g^EV_LmrSYae;VuuaVs+EoSlyv6qFG~gJJnm~a`!&L>P`oo'
    ';2hohUky;*^d?ky>N+yXp#A_<H@ywjonFy%4C)U+b<>+r-Oj{=tDBomAAstnccHq2A((MVXHeZ<r%so)F%zinQ|j~%=cc#8x!Lg~(fkA8-1Ig$cL-BR*BLms'
    '9{iMOtDAyzpB|TS?M3%m9Cv{5;WTb~7mYjAr$G17G;VqujXOFEVIrgY(`elE9vXLO;Hv!LWNvyJnLAMUa2z+i4ae<+vvX7E1905*CLA}U(94J8xan;;Zbq3o'
    '!A;`0S;ukn6povIj1CkcA#+ICQkHcbH@ywV9mM@2aoqGS9Jd%Va}oL(IBt3uj+?;nM6;WZAam2($lUDghvT^EZ8+|f3p){L`V25Py$#GwqoT&|1?J8v=W%Ox'
    '&LWg@3ysT7@8WV>KPbDJ;c`2jDruaDDK7W9>jX>hv{#V09nUoV`4A{Ky$i|>QAaz)b$txw*7K)BYWh0`<<8ffLE^D8U?4&w9m-Abf^w(2mC3pFkk1_kBy(Rc'
    '!2zH9v<)S!MNnRW<j!z_k4AFS+mPH*oS7<^&p>k1+mPJ36a7acx#?X<?u71`z!gj)xwU)C;Ely8BscvCRUtrr5o$<odK;2EA!z0R(+`o{qHC5~$xk4;Q-@b8'
    '$3LJFx#?X*ZnkzNHZVoxw)%D+Pj)gz<W7>M-eW%XKo)SGbR;*u4aprid^(bw-iG96>*doq-1IgMH#KP%_R~4s^ga%E+%fGOQF?;ItqVosWQxO`I-IBAMm*bC'
    'L&gm_-1IgMcRI|>FpfTi!%c7Fa08;9K_Pw!k(=H|<YqE5H=|~V+#ouU+xi(IcXUI=hy&wEFk~FHLgc3R5xIG)ZsAkl-1I&;cl4x|;M^ydqBWem=4tA1?zFQf'
    ')K1E=PqcT@;oS5-I5&Hk2ughloSWVU=MIJ@g_?nL(*Vv5Q*iE7rVtPJ$dED=oZO|{S9ETA7o9u4!2L3v+o^PJ=(;GfsuiYw0|K5f)$p(m=cf0;xkFvh!=Dc4'
    'ruV_QgPUpdI)&**-G>K@NK=^Zw9`jXZ~xvmnyN6}^gc{?$b8}jIJbW50O|AzQ*iD?OGzl1=n^8Qj@ed+bJP3a+=Av81m~u=!MT~-3Y$9}OjU72>fGr8oBK?h'
    'gT_s7qj3XDn7FU`6dE_ZkH+on5{!F*$V6yb!46>Dsk3PmBT(GNN$9RkQpMt?cd@vA!C8DXi<{oY;uffTletf4ansva+>U4!z>Na{H%<Y#<9Y)kj>mPMUNiSe'
    '8)mJK&1(SMDI<T1EB$FKZh9MwJ34h?qN0C_#qHGQTZzTp+{I6^xG8miOcd_qO^DBZ4>cAyeFBS{NO(faITW`(|Dr2SS5e$?0jFaBQu5E>0_iAjdLN40&V`!4'
    '8;YA!BSDVm|7e#8r(0i*#7*xbaa)?YD4QX1d$oglCUJ);5_dXe2WM}8GwLL6dKZbiRGWP^h@0LA;wHq24uhWp;->e3xHHc61jKEh!*GL&;r6o_?y8_HzMy!('
    ';kHTBH*mNa&8s-vDL$z-Qn#}lZi(-M)Lli=IfX#ya6hfsDu<iqxSAiq;eJ}ARSvgX=Wu(K!yP6$+%<;V0Ey1wepaVt4mZORhZ~KWbIS(89EY0&`2DZotP0|O'
    'PNj7ax2;<2>fLa@BZxa8d~9aA#-otP0L1;QWXm9Ksd=p*4&r`Jq!)m=xylUrco6r~Dy@RJ?I3zOL*E9YM^Cw3bR+t<csTw-tJAli7V4GsZ9nH%eFlB|ImKQ<'
    '-?nas<MSc(?Wbj0rf&=5MAxNB`nHxJi3Fam)3+tAVFqylL%Qo>jlcc0ddvLnU}g$YJ_N=6tW4`DZgJkc2*s`6bZl)-r%>D>E?Xdso~%unJDtS+oMtZ|aSPNy'
    '7W?5O?x$s1CUM8(>%<M?hmg3Rm1&*C?dtRy|KTL=r(}8wiQC%`=Wwq`v%=xFARo@&ep;DT_I5m2z$Bvl11Q{2%k)YLcNnj;4}fq#Ez=@|I}Q&Rz#j+UepaSc'
    '2zNTBAC}`Pgxl*7?l1}Aeh|2;Al%Ps_6i8M$6pYH`)Qe$A>3&FOozDd2jupS9VJXo`oDK&OBIm&Y0Vaa++x7|a)8_`Dy;#zxo(&K2|#WM$KN4W?`S~or$t)^'
    'ay!Cb4v_m<nbv{a9F`#5Uf-!0>-N?o2)7UMr~=y`7Eq;dKPS@*DBLzThxlj;_tV-eQ@BHbUkrNtX_a1x-u4Fg7lPh?TCNwOw*@RNqPIauZ^JBl+li#5;8SXL'
    '*|!z;_JU@wV{Z${j5GQf6z=B~dj*BNVTs{~Q@Ed&X_>-JRPDR;ehPOK5TYlOZkH+f87hYRX~|YG+^II<mxJMcR;G0fcXB`{X4Yq5xSx~h6&UUu8Tx4U_S4#|'
    'vbVjpGKT^F1ormRGOe<=A@!b{;ADLUdwW5qHTE_}M{Z`{{tWi^(=siyw;lUA;Qc4Cx1W`1nY|rX9lwOVeY(LPXl7G)pBS*WPiK=~+rnRf+Yap3aNDfr7I<&#'
    '4QUp)E$PZ59{7SwvGd?{-uBa?E%UYm%=8(W<ZY|&0h_u;e#qOF1cyc{f8J&7SGet`HCx4Pr_<gGaob+UZHGzRw#1`MxE0^>9oW6%Z9ga4I&YhCDkuFzc-v2_'
    'w9MP~;3sAJ0N(b~GA;AA6TA2q!`psZre)qX)v-PKaNhQ_GA;AAoe`XyO`pKqep;qg-nOH<^ol;5xBZ+<FXn9%;)esbpO$G2xE)`$b(0hS(ZKDeHChI42cemR'
    '-=_h$pA~Ekxb01Itt%f6+<s1^7XY{8k&w+C-1Y<5+fU21%-#;<Cy}-vz}|jZrd9Sfn@>k?KP}BNdRxNF(A#>3RPHD;iQdkxx*;Ph;&TI44ZZ!eXshUL0r8Wf'
    'eHMEAX~ovi+vy59%v^qc273Exm6p-lC^Y9BydS;&tVrwV?cn`~qqm>dW*NO5>b{Kh;ppuJnHJI8V|e>_0&h$7*wW#NqJ3mU2X8;E(<*q|0s0RIZ$GEaE5O@~'
    'ekSYwGr-$V%d`yMw!(At*2my&QWNSDqgf`w+k&~&*rfndy1~=I+fR$O4BmDI=jPTYz@?v+X&o*d{o3%0flEIv(=uF|hr9SEz@?woW*IIWm)gw1WCkwnRIebx'
    'rD+l_UH98F<h)LoUJz}OF3l`2p-cO-zETylnP=$IXESTfJ$MOFn*B^zd=e-PI#3#Bfzsm8r7BhH2}<u<g(v;AW~)5u;st*;Px@JLUci$MpU;ziTAg*CG}Zg2'
    '`*fc4b3(m@C(VMBWaNiD>7ZWyCZ2TNH*3g~784RUi&FVP7~rIz7Hu6TJy#a=DLCn;g<8i+JN|fv^t0-`fFW&bM#&mOTJK_$xXd*gWyFw9j!Q6xO+YksLN;7#'
    'lK&cr^xO^Ar$D4vgj$10i$z{!NUOc36GNKj7}8=eHL0;sf*4L^NI$LAGDAAfcsnztK7=9toKUY|NC#6FzkCEkTB1JyHlAP@<9`INGo+uEZJ8l$=_3%*Y^@})'
    'd;lR|8n!A7>8GVyW=N-e<zEm(`dOjY8PXmBXM!$s3~4Q$JTasRCK=L_Pi5P`IqIk}q@R{;nIRn=YMI%=z9UVVCB#!&+%zO{2aVTh($C5E3Yv6$aq%yRCjGQf'
    '>ojRW`E;E0)9S3_q#36+tq;LTKP%J=aMCPmJZaJ0P>k<7ndC{Qj)8<eRlKAML)3xN&uR4vptRm2=~H;pPiwQzlTHm`CIVF-!jpblsAZnC=b1G8DYUen9h^u}'
    'WT(*5r}v|4?O0wzlV+kxp+1Zz{j_H5G-;}B=S7;dcK4TP(#_<Wq)Ep&&4!XrebrEhNk1##YhlvfgxA5O-2f(SufwEo9O?Wgq1R#3&uRG*n6#@Og;rqFgIuBn'
    'lNLJ(lde$GQ6eCnCjGQ*>ojR!Cx+Nnnl$J%X>TTJ(gMcsfAd|ZjtVFJoNO<_NjqadohSXYI_o@X>wif+>8FKS=1JRf%<g&8|JT3!yZ^(W`Ql1{aizbw(qCNZ'
    'FRt_#SNe-9{l%63;!1yUr9X%(U3!^-!j&$ggIGj>rYeWgTyWk?2HJ{Z#r6?bx&@9#U%9z)<#-C6xpDo%Z-3#pzwp~%`0X$J_7{Hp3%~t^-~PgHzXZSShtlZL'
    'bxOaYx&gg|OK^-=;J5u)n)-0;qJJfwG&n_l_@a~#l%bq%sVPH!GG?m`L7f2!a-pqI{IppFCU4dal%cwHtm_(8xcHl$i2;{yBAqauJYKKnZxe;OG$*cSy1;Ue'
    '$5U8ViH2Bdk{Xa@UH-k)x$QF7i~kFG|AoB&Lf(HN@4t}uU&#9}<oy@&{tJ2kg}nbl-hUzQzmWI;|3coov)iY7l+*<F=!?{KNyz#Xf<&+OV0@}dvi-{rd9A<o'
    '{uuf$E?8%*BSX83qfm`zwIjv+St<NJ%DMVm%^;)tY8@$<rzjY>_}bf_LKiF$RL%hsInfC2-qh&Gk#yYO^YqeL^_&w8sxvkadHx89r3?LvT*Y!bkc*+-qT2Gn'
    '_=^r^a(YQWlN*g>bXgovPc@bt{Dm6%_+ehuNct|Ey+*-QQws8(!TFQ|0gGdiFz6GhROKoY$r)QG;h_muZNT6}(gy2ORZ{T=nrUSMu4~ryHSKa04RICpOa(ty'
    'TZYg?_QHVUzU640Iu5jTR#xRBtbrNv4R9fVj$y6E)nU61;f`;-`h^&}%&F9(A7>(altY|WiR|s8O6H=o)>Y}%=;Z>yCuLgGqD;j)Jqb42SH+3Z+u*ww;#9{p'
    'psmBDDideSwG!D&&E#wPA?ig(Q9y|Pamo{-<h|a(>(+qW`*Y}$rM&Y7@#k91Z7S!RISJB92iFG3U2?6J8we-dFpn%<XlEQhz1Gfnw_R;25bbM7)A6<4wWdhN'
    'nu(cZx7w@o1CG6w1S$t+7i<IMhAWl34%RAn58L&9Do}%lGT2jEbG%X4hKAZu?mDZHlH=jDZc~UhT)Hb>qj2xG53~`2=BlsnZer~e^vG`cYetjFo?S{DG7&YD'
    'NH!N><>F;kreeD)lQ+3MwW372_DU2WOkJtxuOg#Rdu%>k>18qAggW8XMj!M7b2ON>efws+o(bg*`$i^g+t53Vj@-<?ZL_<k^jz3Ry*`_e>tIy6y|Y*D`qpw+'
    'cH7lT<b>*K(X4HXk448**l4>oD@J8D0iN|~+Rd=s?Axgz@xtC@)|jI=+x4h<w6A%+L9Weg8dlY(VLyW1gWkB`DI5H5hBtNNrPSc;!rlaaZ42LR*ZKCY;fhf3'
    'ZL>=oco4J8Pv3Q!>9k30>9q~X+j>Ky=rGAlGH|*`iwC~o(v8erd3OAtp|B`gc__7l&--XWC7R^kXq9Lt%q4HQTi$^-w^D_@?jEqIo5NU>TWBew5vLo-^5<Qd'
    'TKrMqMa^7KmFYA~ZH}sKEBaAgUINz0RvBoPL(!^cP*|TD?cQ)53r$^lVPeh&%bY02B5~W0yF3_LU8dV3>2!CKfdf_MLVIuOFgTcqDU{X&8YIczD%O;H8V0sa'
    'S|^K6L5SzTaBJGZ_uXmJOk87_Z%?AST@1cql4P)P-6R^PD%jFD6}ko}nw3#aGx9YL!8v0u90Of0G+0vUjttVNM6F7xe!TVJJ@`qP5(!)hFzln+x{Sx;8qUpf'
    ';r=FmdhPyZw@wWzB>NhKIdmO-4FgQd6x2pn`>0j{!tvKpZ(pga>jjx$c{YV{pQ~tJldtD9TBdd<s1A(EK&F)HZj@xJGOD+G;nGm<2v<3Cf%T5rqh66|vt3_w'
    'zG+}$LE368>x|&sY=RsGKKJ3vsLq$<G-qp>a%^r|CbWx-uGp^o@X<cg9(CH(m-ynX3$^w50-q4T51I04N^4Y3gz?R9>(srFUtP2534!g>Y}##CFCU_PRHr8d'
    'i(M8+x=NWi>hO*J$BIn;j6=CoF5|DX3B#aVSagDZRi<LQ8t{zv)$oxNnbPjH;x>gilIf1$(a6MECe~!~b*4ge<SrbX6H9SbreeE}m^Pl9H=XHTbXZ@J4kwf7'
    'sl@xJ5<Qo=@@sz}*b5>BYZo<=aUyZDuI{!jV%ks@1fjVV2d90@s!`C3WQ4Hk=aQPTJOwV#O-5G)+nz#w;R!)a;>;!6b<3M3y<)`U-a2lBf-Q#;dPJ<L<HU~W'
    '!d+kd^x9qDZu>waseMd`9=W@bF+6i6Dz{gLS7a)q+6Gm6s#gdSywE?1%~C6g!#bU&9PMl1+zvZFOCUc{3JSSu9)faW*+;b<%30vLnG(7S9Gj5q@0Vn<eWUzN'
    'rf6T?dS1F7dBA2UP445SN>fI)h32HrJ#dn5>E@Z!YguNybpvUnZc6D&zG;nPnL{C^ukzW`7e%JhwOC`Ar#5XhXUiL%WspuZhK#U?FKJROb<4>j#C&@aHRvhc'
    'as#}Bu6Ra3{G@1{uQ;^1C4DTc5sfn_<Jg_j5%ZaM9{c5KRvts2W^9^imair2w;t55y`f+sy8%WKES#b64l6=^QIm?${gsmSC0gljoqp|??5n$Rf$Mr}q)ebW'
    '=O7-tHFC`{s`*mFO=Z8iP*od0yttWfw&@P1G&LY`gKEQ19)kBHW%Ld}WfXKSw@9cfKjJoZ1=!}jUDf762cT#lH2whfG{8{zy{55Dpx__2Q9YO<!2mYYM7%^z'
    'T)Q{kZI^*e^|{VD?s^`uvm<G8Dc`wA8Mag?4a=S0rghubsLY@!CZx2IK+(R8Dem?o#KrgA_8_`@8GO@R*kHHR91Jn<B`x!O4uxB13S|=t#(8KQC%WV~Ez!6S'
    '+toP~?Q0m4g3xj782wDWP}tgOo!>)+twXiuI0a}L{5Z+Eqti0wb-E;z9p5|!XfD;$9T>H{wH2OddJbW0k9Zzw3b&A#Hk;7-rdbV<3@+7E#|@VPOzD>RZr#!H'
    '-12>eeJ2{)Iz*5f9k=chIfHX^Ys0oGqHUuZ>AmzqJUWzFxWIx!TIz~>Sf|UBcwtQ|GU?nUPcq{!<yJ>sQhOs4_GJQ%OwKhRb95K7TjQtKvRilC^(E-6G?e#@'
    '&8ubN;VurS=lqmWuXYcPFEFgz!pFDHh0d-9mbjXS?Wk)b&6RHuZX0j{xv8A;a7!{iW%-%x6*$!^2;MZP+n`?g(vNi$V?*u^<rfw&fWt|ucQ0B9^`0I)b}>xm'
    'nRSQe<VwB~&7XCl+|7`Dqk44Ezd&YKa}RdI^`l%g&NLnK^D?hHyT(KJL|8n1)hPv~9<^${V}jgD+ca2DQ*r20eZBPr1t`#W5cZ3jl?T!1Rj|H+92w{k4P-`>'
    'DVN48WTV@t3O<aU5H|peAXfku!7omz&3L^Dbu`d5+j#!UdPbH1LRBYsJ3EIJnJtG@MeQVaqkhK$@P*X|uvEQ&7(d`6r@ZmnW%AJT7V3S|4WQ*Cs?%*$g^C#r'
    'SRY$F<CO>jn?<DiLwHj(5SrMfZOe*X+`DmxLIhi5uXA8b?oyGOLuhnllU%c=CJ6P6O%oE9lv0UHaQF2=%c7MB(K~h<;`+YarA8y&b<|YpS^6$Mi%@o>8fC{M'
    'ws3vgyHd&MWUZ3%Fi-CRpA4*1&Es#bg++p69H^68G6m&bcN<l^m>Y)Rny?dJ3OlWV({|%^1hOfu(vP^4^x-*MgVXF-C{&7swo$$RR<r}_j)3uepf0`SxCCR^'
    '1Vlr|-Yvc*^3d@hG!PA&K#O(KRWHolP;K8O>e@@AVX*cJX}A*YO6<4ve_ItwhHH<>*TLU-{_ZY=E%Hd6(hgNqCwKi)0Ujn!WomB92MBje?X@hFAcniYHusB4'
    'l?NFpRc<+}YjbYbVVv=HW=dgBWH2iDzKp6o0VCejm&43gwsZDt+xc$%z*&%d4|_!Ks7v(?rY^9;kx->sn<f;UbtpOP2%+xfX43&#onFx&02@y*jPXB$zpGFP'
    'OF@O>Q3kSw&@@-<uvT#|(6Lrnx>9KP-&@ZLEfoU*p=)ExtdqB`1dhjf5b~wHd)L&|tx8oMMPG`5M(Xr2*8zxfTK5dtHu=5kmFF?4xubE$aI?F^g^Ey@YT4F='
    '+RPgW)dJ;QAtu!>D8dx?9&<Po)TLO0om$p?v;zs6e&%+t)2CC}_HT|l_T9~wy1^cgA|u&CEgN#zgE>0XGw2wkTh7|4Z(2t6X8G|1oa6Mz{6eQExJZbnw76-A'
    'cO|d1n{Bv+7&sn<2eM^Q#4+2rFK0KFXCTRR>yhLX*~+LEm5Gb8t{L^#T!Fq@vj%<N&C^eY(ZGf>K!qY&MP&qKq)x_2o@SYsQ35??1tN83uovVD7c_!P49vrL'
    'ud5@@HRLpEeRl@NsZEQvjDvGhbRIn^BswT}CU>_wpnP&ASnOqWy=}(phn!T$-J+Y?Zd_hi>_{Cv>j2-4s!(^+USL^6{+T?o_=8KOvxlX2Ae5`?jXik}8a&Ne'
    'YbLfRyYUy!Ux;N?bSEC8r%;F3OJ8=);WB~Hr?RS41B`b92Dfzf*&k&9Z-*A%Ze4qK<Ml3PnA@PFx13d!qI{NDl|pP_+NCmWdKKbQX4zr<U=?C^NDj`NZ%d0)'
    'XKg<d=Cd0uQJqp_(j$_qtJyudD{nn=Yj3^X_`xJ1$IV->EnxT7^Gp##`6{gfbQ{%ks}sDGYMsoLhDq_$OAV6_<8h#q)IB`oVY+44dtjnTp*V-rwF#9+LEYiw'
    '0R<b1?}c)Y`01r`kHh%Egi7s|YoR(vAqwSbBI6}}SI9llydqR4QmfZD(IrCP<Z`fk>Qxy3hgNs;@*Gar%vo-(OU1QUUhxDGW{%D_#}-MsIW41ZFuA;GR$;V*'
    'Ci+N3hbCbzQZa_f>EL~~H7)L9+e4XwfT3BPQ0K;Wz65zQbKG&m+e3G20H+0SkaL5+!<Rswk}@@xaND1RcNuFI*RD3)<7>redE-`4=&_KleiRxh$hxQRlx&Ct'
    ')oe0tbcgqG(AzUII}{VWH{mD|^z9kr&n___jz`fwj(A+G*Ko3^W%&hP=O5XTY$3k_NRe&KPbRSSIXiU_zkt)^Tt~4aR54zElSTt;s0eq+!(e2lfypL>k`TF#'
    '`T?dKTwVUo9(1IEhs4O({<Ly8yZmBO#QrES60J_uim$S^3w>&O05k4~%T*al&U+e#vv&1|9hmdf4%z6r17`fewFBn<9Qs=|+G=g1S(|3V>5^vVRZzO}C`9vZ'
    ')DKo91z+bQiQ4dzeiSf%|C{gHfW@C(YRYsxiheGS=GM%Sl#JBu|7_e~S^5#VZ5!1&l0XicgQX>ZA-rSBuB!6@-Ife&3Ms7VDo|6ep@~ic>AU32gQ!PYKQIv^'
    'HA!yyygPlc%vks(p^EVXp*W}fR!MH5O93qL)tfs1^grW=|MahaP8R-;U;Xsw-~Qjf|8GD4=C}X)*FXL9Kkr`}^3ApUEr#t&fBVxPfA{;}{q)zL5Bq=k-{FUU'
    '`9u7#e)`uRe)_{7e)#FPzy0}N{`&Lpe)#L3fBN0ef2n`yH^2SUfBX5*KmFIAfAfc*{_@xQTiP6=-~K6&?mzbf{_roq|Cb+r`d$3#zx@0+KiK^L{^hT6{HH(s'
    '>5o6h|M=&h|MHg~{`AK`{J-(XfB(nd{_FqqU*hlnFY$lmzx<5uhrj!WfB%oa{)fN+;phMULsI-d{m1|HhyVIhvN3A^zx}&k|Hps);eYwzr+@t&@(=&v*Z=Sz'
    '|HHriyMO$5|L4Dr|KC0we<kHV{px@I&;Jh@rIk<'
)
EXPECT = {'CONTROL_30_RR4': {'source_config_id': 'EURAUD28_FROZEN30_RR_4_RR4.00', 'lookback': 30, 'body_atr_min': 1.0, 'wick_body_min': 0.25, 'rr': 4.0, 'count': 96, 'r': 47.80685132075335, 'four_pip_standalone_r': 42.16823877518033, 'four_pip_standalone_pf': 1.6389127087148534}, 'LB25_RR2_5': {'source_config_id': 'EURAUD28_LB25_RR_2.5_RR2.50', 'lookback': 25, 'body_atr_min': 1.0, 'wick_body_min': 0.25, 'rr': 2.5, 'count': 106, 'r': 41.73519048992437, 'four_pip_standalone_r': 36.04326671004743, 'four_pip_standalone_pf': 1.5813430114523779}, 'LB25_RR3_5': {'source_config_id': 'EURAUD28_LB25_RR_3.5_RR3.50', 'lookback': 25, 'body_atr_min': 1.0, 'wick_body_min': 0.25, 'rr': 3.5, 'count': 106, 'r': 53.647915196669246, 'four_pip_standalone_r': 47.4236687510821, 'four_pip_standalone_pf': 1.687299547117132}, 'LB25_RR4': {'source_config_id': 'EURAUD28_LB25_RR_4_RR4.00', 'lookback': 25, 'body_atr_min': 1.0, 'wick_body_min': 0.25, 'rr': 4.0, 'count': 105, 'r': 57.90421353631643, 'four_pip_standalone_r': 51.4581780201125, 'four_pip_standalone_pf': 1.72476307070581}, 'LB20_RR4': {'source_config_id': 'EURAUD28_LB25_LOOKBACK_LB25_20_RR4.00', 'lookback': 20, 'body_atr_min': 1.0, 'wick_body_min': 0.25, 'rr': 4.0, 'count': 127, 'r': 54.57382928525179, 'four_pip_standalone_r': 47.05192374889216, 'four_pip_standalone_pf': 1.5286733005493502}, 'LB25_WICK020_RR4': {'source_config_id': 'EURAUD28_LB25_WICK_LB25_0.2_RR4.00', 'lookback': 25, 'body_atr_min': 1.0, 'wick_body_min': 0.2, 'rr': 4.0, 'count': 118, 'r': 59.085699441239335, 'four_pip_standalone_r': 51.931313274544294, 'four_pip_standalone_pf': 1.64112732437709}}
CUTOFF = "2026-09-20T18:29:00Z"
EXPECTED_BASELINE = {
    'trades': 3029, 'strategies': 27,
    'weighted_r': 1763.8084030796124,
    'ending_balance': 1731448888.7485507,
    'closed_dd': -17.089509091188603,
    'floor_dd': -17.84461250071128,
    'max_positions': 6,
    'max_open_risk_pct': 5.903440932560884,
}
EXACT_CONFIGS = ('CONTROL_30_RR4', 'LB25_RR2_5', 'LB25_RR3_5',
                 'LB25_RR4', 'LB20_RR4', 'LB25_WICK020_RR4')
OUTPUT = Path(os.getenv('EURAUD28_OUTPUT_DIR', '/tmp/euraud28_exact_portfolio_add'))
RESULT_ZIP = OUTPUT / 'EURAUD_H1_LONG_27_TO_28_EXACT_PORTFOLIO_ADD_RESULTS.zip'
STATE = {'state': 'idle', 'progress': 0, 'message': 'Not started',
         'orders_supported': False, 'trading_enabled': False,
         'portfolio_source_cutoff': CUTOFF}
LOCK = threading.RLock()

def status(state, progress, message):
    with LOCK:
        STATE.update(state=state, progress=progress, message=message)

def fail(message):
    with LOCK:
        STATE.update(state='failed', message=message, progress=0)

def stamp(v):
    if isinstance(v, dt.datetime):
        return v.isoformat().replace('+00:00', 'Z')
    return v

def parse(v):
    return dt.datetime.fromisoformat(v.replace('Z', '+00:00'))

def write_csv(path, rows, fields=None):
    rows = list(rows)
    if not rows and fields is None:
        rows = [{'no_rows': True}]
    if fields is None:
        fields = list(dict.fromkeys(k for row in rows for k in row.keys()))
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)

def strict_close(a, b, *, label, abs_tol=1e-7, rel_tol=1e-12):
    if not math.isclose(float(a), float(b), abs_tol=abs_tol, rel_tol=rel_tol):
        raise RuntimeError(f'PARITY FAILURE {label}: {a} != {b}')

def load_archive():
    raw = zlib.decompress(base64.b85decode(PAYLOAD_B85))
    digest = hashlib.sha256(raw).hexdigest()
    if digest != PAYLOAD_SHA256:
        raise RuntimeError('Embedded source-data SHA-256 mismatch: refusing analysis')
    data = json.loads(raw)
    if data['source']['archive_cutoff'] != CUTOFF:
        raise RuntimeError('Archive cutoff drift')
    if set(data['candidates']) != set(EXACT_CONFIGS) or len(data['candidates']) != len(EXACT_CONFIGS):
        raise RuntimeError('Predeclared finalist set drift')
    return data, digest

def identity(t):
    return (t['sid'], t['pair'], t['side'], t['tf'], t['signal'],
            t['entry'], t['exit'], round(float(t['r']), 9), float(t['rr']))

def candidate_identity(t):
    return (int(t['signal_index']), int(t['exit_index']),
            t['entry'], t['exit'], round(float(t['r']), 9))

def check_standalone(data):
    audits = []
    for label in EXACT_CONFIGS:
        rows = data['candidates'][label]
        e = EXPECT[label]
        if len(rows) != e['count']:
            raise RuntimeError(f'{label}: standalone count parity failed')
        strict_close(sum(float(x['r']) for x in rows), e['r'], label=f'{label} standalone R')
        if len({int(x['signal_index']) for x in rows}) != len(rows):
            raise RuntimeError(f'{label}: duplicate standalone signal indices')
        if [x['entry'] for x in rows] != sorted(x['entry'] for x in rows):
            raise RuntimeError(f'{label}: standalone chronology out of order')
        for r in rows:
            if r['pair'] != 'EUR_AUD' or r['side'] != 'BUY' or r['tf'] != 'H1':
                raise RuntimeError(f'{label}: strategy identity drift')
            if not parse(r['signal']) + dt.timedelta(hours=1) == parse(r['entry']):
                raise RuntimeError(f'{label}: H1 entry timing/lookahead failure')
            if not parse(r['exit']) > parse(r['entry']):
                raise RuntimeError(f'{label}: exit before entry')
            if r['exit'] > CUTOFF:
                raise RuntimeError(f'{label}: candidate exit exceeds frozen portfolio cutoff')
            if float(r['rr']) != e['rr']:
                raise RuntimeError(f'{label}: RR changed')
        audits.append({'candidate': label, 'source_config_id': e['source_config_id'],
                       'lookback': e['lookback'], 'body_atr_min': e['body_atr_min'],
                       'wick_body_min': e['wick_body_min'], 'rr': e['rr'],
                       'standalone_trades': len(rows),
                       'standalone_r_2pip': round(sum(r['r'] for r in rows), 9),
                       'standalone_r_4pip_source_only': e['four_pip_standalone_r'],
                       'standalone_pf_4pip_source_only': e['four_pip_standalone_pf'],
                       'parity': 'PASS'})
    return audits

def all_events(trades):
    events = []
    for i, trade in enumerate(sorted(trades, key=lambda t: (t['entry'], t['sid']))):
        events.append((trade['entry'], 1, trade['sid'], i, trade))
        events.append((trade['exit'], 0, trade['sid'], i, trade))
    events.sort(key=lambda a: (a[0], a[1], a[2], a[3]))  # exit before entry
    return events

def equity(trades, include_events=False):
    ordered = sorted(trades, key=lambda t: (t['entry'], t['sid']))
    balance = 100.0
    peak = 100.0
    max_closed = 0.0
    max_floor = 0.0
    max_open = 0
    max_open_risk = 0.0
    at_exit = []
    open_positions = {}
    outstanding_risk = 0.0
    samples = []
    candidate_with_audusd = 0
    candidate_with_eurgbp = 0
    candidate_with_eurusd = 0
    candidate_with_others = 0
    candidate_entries = 0
    for when, kind, sid, idx, t in all_events(ordered):
        if kind == 0:
            if idx not in open_positions:
                raise RuntimeError(f'Exit without entry {sid} {when}')
            risk_cash, tr = open_positions.pop(idx)
            outstanding_risk -= risk_cash
            balance += risk_cash * float(t['r'])
            if balance <= 0:
                raise RuntimeError('Non-positive backtest equity')
            peak = max(peak, balance)
            max_closed = min(max_closed, 100 * (balance / peak - 1))
            at_exit.append((when, balance, sid, t['r'], risk_cash))
        else:
            rp = .0075 if sid == 'EUR_JPY_M15_SHORT' else .01
            risk_cash = balance * rp
            outstanding_risk += risk_cash
            open_positions[idx] = (risk_cash, t)
            if sid == 'EUR_AUD_H1_LONG':
                candidate_entries += 1
                if any(v['pair']=='AUD_USD' for _,v in open_positions.values() if v is not t):
                    candidate_with_audusd += 1
                if any(v['pair']=='EUR_GBP' for _,v in open_positions.values() if v is not t):
                    candidate_with_eurgbp += 1
                if any(v['pair']=='EUR_USD' for _,v in open_positions.values() if v is not t):
                    candidate_with_eurusd += 1
                if len(open_positions) > 1:
                    candidate_with_others += 1
        max_open = max(max_open, len(open_positions))
        max_open_risk = max(max_open_risk, 100 * outstanding_risk / balance)
        max_floor = min(max_floor, 100 * ((balance-outstanding_risk)/peak-1))
        if include_events:
            samples.append({'event_time':when,'kind':'EXIT' if kind==0 else 'ENTRY',
                            'strategy':sid,'realised_equity':balance,
                            'open_positions':len(open_positions),
                            'open_risk_cash':outstanding_risk,
                            'open_risk_pct':100*outstanding_risk/balance,
                            'closed_dd_pct':100*(balance/peak-1),
                            'floor_dd_pct':100*((balance-outstanding_risk)/peak-1)})
    return {'balance':balance, 'max_closed_dd':max_closed,
            'max_floor_dd':max_floor,'max_open':max_open,
            'max_open_risk_pct':max_open_risk,'exit_events':at_exit,
            'event_rows':samples, 'candidate_entries':candidate_entries,
            'candidate_overlap_any':candidate_with_others,
            'candidate_overlap_audusd':candidate_with_audusd,
            'candidate_overlap_eurgbp':candidate_with_eurgbp,
            'candidate_overlap_eurusd':candidate_with_eurusd}

def balance_at(sim, when):
    times = sim['exit_times']
    ix = bisect.bisect_right(times, when) - 1
    return sim['exit_balances'][ix] if ix >= 0 else 100.0

def add_balance_index(sim):
    sim['exit_times'] = [x[0] for x in sim['exit_events']]
    sim['exit_balances'] = [x[1] for x in sim['exit_events']]
    return sim

def calendar_anchor(now, yrs):
    return now - dt.timedelta(days=365.2425*yrs)

def cagr(sim, first, last):
    years = (last-first).total_seconds()/(365.2425*86400)
    return ((sim['balance']/100.)**(1/years)-1)*100 if years>0 else None

def summary(label, trades, sim, first, last):
    winner=[t for t in trades if t['r']>0]
    loser=[t for t in trades if t['r']<0]
    weighted_r=sum(t['r']*(.75 if t['sid']=='EUR_JPY_M15_SHORT' else 1.) for t in trades)
    return {'candidate':label,'portfolio_strategies':len({t['sid'] for t in trades}),
            'accepted_trades':len(trades),'accepted_candidate':sim['candidate_entries'],
            'candidate_r':sum(t['r'] for t in trades if t['sid']=='EUR_AUD_H1_LONG'),
            'weighted_r_equivalent_at_1pct':weighted_r,'winners':len(winner),'losers':len(loser),
            'win_rate_pct':100*len(winner)/len(trades),
            'profit_factor':sum(t['r'] for t in winner)/abs(sum(t['r'] for t in loser)),
            'ending_balance_from_100':sim['balance'],'historical_cagr_pct':cagr(sim,first,max(parse(t['exit']) for t in trades)),
            'max_closed_equity_dd_pct':sim['max_closed_dd'],
            'max_open_risk_floor_dd_pct':sim['max_floor_dd'],
            'max_open_positions':sim['max_open'],
            'max_open_risk_pct_of_realised_equity':sim['max_open_risk_pct'],
            'candidate_entries_overlapping_any_incumbent':sim['candidate_overlap_any'],
            'candidate_entries_overlapping_AUD_USD':sim['candidate_overlap_audusd'],
            'candidate_entries_overlapping_EUR_GBP':sim['candidate_overlap_eurgbp'],
            'candidate_entries_overlapping_EUR_USD':sim['candidate_overlap_eurusd'],
            'source_cutoff':CUTOFF}

def period_rows(label, sim, cutoff):
    out=[]
    for years in (1,2,3,5):
        start=calendar_anchor(cutoff,years)
        a=balance_at(sim, stamp(start)); b=balance_at(sim,CUTOFF)
        out.append({'candidate':label,'period':f'LAST_{years}Y',
                    'start':stamp(start),'end':CUTOFF,'start_balance':a,'end_balance':b,
                    'compounded_return_pct':100*(b/a-1),
                    'annualized_return_pct':100*((b/a)**(1/years)-1)})
    return out

def monthly_rows(label,sim,first,cutoff):
    out=[];y=first.year;m=first.month
    while (y,m)<=(cutoff.year,cutoff.month):
        start=dt.datetime(y,m,1,tzinfo=dt.timezone.utc)
        yy,mm=(y+1,1) if m==12 else (y,m+1)
        end=min(dt.datetime(yy,mm,1,tzinfo=dt.timezone.utc),cutoff)
        a=balance_at(sim,stamp(start)); b=balance_at(sim,stamp(end))
        out.append({'candidate':label,'month':f'{y:04d}-{m:02d}',
                    'period_start':stamp(start),'period_end':stamp(end),
                    'partial_month':y==cutoff.year and m==cutoff.month,
                    'start_balance':a,'end_balance':b,
                    'return_pct':100*(b/a-1)})
        y,m=yy,mm
    return out

def rolling_rows(label,sim,first,cutoff):
    months=monthly_rows(label,sim,first,cutoff)
    out=[]
    for span in (12,24,36):
        for ix in range(span-1,len(months)):
            start=months[ix-span+1]['period_start'];end=months[ix]['period_end']
            a=balance_at(sim,start);b=balance_at(sim,end)
            out.append({'candidate':label,'months':span,'start':start,'end':end,
                        'includes_partial_end_month':months[ix]['partial_month'],
                        'compounded_return_pct':100*(b/a-1)})
    return out

def rolling_summary_rows(rows):
    grouped=defaultdict(list)
    for r in rows:
        # Omit partial-September end month from comparable completed windows.
        if not r['includes_partial_end_month']:
            grouped[(r['candidate'],r['months'])].append(float(r['compounded_return_pct']))
    return [{'candidate':candidate,'months':n,'completed_windows':len(vals),
             'pct_positive':100*sum(v>0 for v in vals)/len(vals),
             'worst_return_pct':min(vals),'median_return_pct':statistics.median(vals),
             'best_return_pct':max(vals)}
            for (candidate,n),vals in sorted(grouped.items()) if vals]

def yearly_rows(month_rows):
    grouped=defaultdict(list)
    for m in month_rows:
        grouped[(m['candidate'],m['month'][:4])].append(m)
    out=[]
    for (label,year),months in sorted(grouped.items()):
        ret=100*(months[-1]['end_balance']/months[0]['start_balance']-1)
        out.append({'candidate':label,'year':year,
                    'months_in_year':len(months),
                    'partial_year':year=='2026',
                    'compounded_return_pct':ret})
    return out

def run():
    try:
        OUTPUT.mkdir(parents=True, exist_ok=True)
        status('loading',5,'Verifying embedded archived 27-trade and EUR/AUD finalist ledgers')
        data,digest=load_archive()
        base=data['baseline']
        if len(base)!=3029 or len({t['sid'] for t in base})!=27:
            raise RuntimeError('27 baseline count or strategy ID parity failed')
        if any(t['entry']>CUTOFF or t['exit']>CUTOFF for t in base):
            raise RuntimeError('Incumbent row newer than source cutoff')
        ids=[identity(t) for t in base]
        if len(set(ids))!=len(ids):
            raise RuntimeError('Duplicate incumbent trades')
        strict_close(sum(t['r']*(.75 if t['sid']=='EUR_JPY_M15_SHORT' else 1) for t in base),
                     EXPECTED_BASELINE['weighted_r'],label='baseline weighted R')
        standalone=check_standalone(data)
        status('baseline',17,'Replaying full event-driven current 27 portfolio for exact metric parity')
        sim0=add_balance_index(equity(base,include_events=False))
        if len(base)!=EXPECTED_BASELINE['trades']:
            raise RuntimeError('3029 trade parity failed')
        for key,a,b in [('ending_balance',sim0['balance'],EXPECTED_BASELINE['ending_balance']),
                        ('closed_dd',sim0['max_closed_dd'],EXPECTED_BASELINE['closed_dd']),
                        ('floor_dd',sim0['max_floor_dd'],EXPECTED_BASELINE['floor_dd']),
                        ('max_positions',sim0['max_open'],EXPECTED_BASELINE['max_positions']),
                        ('max_open_risk_pct',sim0['max_open_risk_pct'],EXPECTED_BASELINE['max_open_risk_pct'])]:
            strict_close(a,b,label='baseline '+key)
        first=parse(base[0]['entry']);cutoff=parse(CUTOFF)
        baseline_summary=summary('CONTROL27',base,sim0,first,cutoff)
        strict_close(baseline_summary['historical_cagr_pct'],115.53348121349032,label='baseline exact published CAGR')
        source_unchanged=hashlib.sha256(json.dumps(ids,separators=(',',':')).encode()).hexdigest()
        baseline_parity=[{'check':'embedded_payload_SHA256','actual':digest,'expected':PAYLOAD_SHA256,'pass':True},
                         {'check':'incumbent_trade_identity_SHA256','actual':source_unchanged,'expected':source_unchanged,'pass':True}]
        for label,actual,expected in [('trades',len(base),3029),('strategies',27,27),
          ('weighted_r',baseline_summary['weighted_r_equivalent_at_1pct'],EXPECTED_BASELINE['weighted_r']),
          ('historical_cagr',baseline_summary['historical_cagr_pct'],115.53348121349032),
          ('ending_balance',sim0['balance'],EXPECTED_BASELINE['ending_balance']),
          ('closed_dd',sim0['max_closed_dd'],EXPECTED_BASELINE['closed_dd']),
          ('floor_dd',sim0['max_floor_dd'],EXPECTED_BASELINE['floor_dd']),
          ('max_positions',sim0['max_open'],EXPECTED_BASELINE['max_positions']),
          ('max_open_risk_pct',sim0['max_open_risk_pct'],EXPECTED_BASELINE['max_open_risk_pct'])]:
            baseline_parity.append({'check':label,'actual':actual,'expected':expected,'pass':True})
        summaries=[baseline_summary];parities=[];periods=period_rows('CONTROL27',sim0,cutoff)
        monthlies=monthly_rows('CONTROL27',sim0,first,cutoff)
        rollings=rolling_rows('CONTROL27',sim0,first,cutoff)
        trades_all=[dict(t,candidate='CONTROL27',risk_pct=(.75 if t['sid']=='EUR_JPY_M15_SHORT' else 1.)) for t in base]
        deltas=[];events=[];notes=[]
        for number,label in enumerate(EXACT_CONFIGS,1):
            status('portfolio',17+number*11,f'Exact 27→28 test: {label} ({number}/6)')
            candidate=data['candidates'][label]
            if any(t['pair']=='EUR_AUD' for t in base):
                raise RuntimeError('New instrument is already in 27 baseline; separate pair-gate replay required')
            prospective=sorted(base+candidate,key=lambda x:(x['entry'],x['sid']))
            survivors=[identity(t) for t in prospective if t['sid']!='EUR_AUD_H1_LONG']
            if survivors!=ids:
                raise RuntimeError(f'INCUMBENT PARITY FAILURE {label}: baseline trades changed')
            added=[t for t in prospective if t['sid']=='EUR_AUD_H1_LONG']
            if len(added)!=EXPECT[label]['count']:
                raise RuntimeError(f'{label}: accepted additions do not equal standalone set')
            if [candidate_identity(t) for t in added]!=[candidate_identity(t) for t in candidate]:
                raise RuntimeError(f'{label}: candidate p0 / entry-exit chronology drift')
            if len(prospective)!=3029+len(candidate):
                raise RuntimeError(f'{label}: full accepted count mismatch')
            sim=add_balance_index(equity(prospective,include_events=True))
            s=summary(label,prospective,sim,first,cutoff)
            summaries.append(s)
            for key in ('accepted_trades','weighted_r_equivalent_at_1pct','historical_cagr_pct',
                        'ending_balance_from_100','max_closed_equity_dd_pct',
                        'max_open_risk_floor_dd_pct','max_open_positions',
                        'max_open_risk_pct_of_realised_equity'):
                before=baseline_summary[key];after=s[key]
                deltas.append({'candidate':label,'metric':key,'control27':before,
                               'prospective28':after,'delta':after-before})
            parities.append({'candidate':label,'incumbent_trade_count':len(survivors),
                             'reference_incumbent_trades':len(ids),
                             'incumbent_trade_identity_SHA256':source_unchanged,
                             'candidate_raw_and_accepted':len(candidate),
                             'candidate_blocked_by_same_pair_nonhedging':0,
                             'candidate_displaced_incumbent':0,
                             'candidate_1pct_allocation':True,
                             'incumbent_parity':'PASS'})
            periods+=period_rows(label,sim,cutoff)
            monthlies+=monthly_rows(label,sim,first,cutoff)
            rollings+=rolling_rows(label,sim,first,cutoff)
            trades_all += [dict(t,candidate=label,risk_pct=1.) for t in candidate]
            for e in sim['event_rows']:
                if e['strategy']=='EUR_AUD_H1_LONG':
                    events.append(dict(e,candidate=label))
            notes.append({'candidate':label,'assessment':'REPORT_ONLY',
                'condition':'Exact recorded candidate adds to new instrument, no historical EUR_AUD incumbent to block',
                'cost_stress':'4-pip values are source standalone only; no exact four-pip portfolio simulation',
                'live_interpair_currency_cap':'NONE; simultaneous overlap and open risk are reported'})
        # Never auto-select or enable a strategy based on in-sample results.
        status('export',91,'Writing parity, attribution, period and risk-event reports')
        files={'baseline_parity':baseline_parity,'standalone_parity':standalone,
               'incumbent_parity':parities,'portfolio_summary':summaries,
               'delta_vs_27':deltas,'portfolio_periods':periods,
               'monthly_returns':monthlies,'rolling_returns':rollings,
               'rolling_summary':rolling_summary_rows(rollings),
               'calendar_years':yearly_rows(monthlies),
               'all_trade_entries':trades_all,'candidate_risk_events':events,'research_notes':notes}
        paths=[]
        for name,rows in files.items():
            path=OUTPUT/f'euraud28_{name}.csv';write_csv(path,rows);paths.append(path)
        with zipfile.ZipFile(RESULT_ZIP,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=7) as z:
            for path in paths:z.write(path,arcname=path.name)
        status('complete',100,f'Completed 6 fixed finalists. Archive: {RESULT_ZIP.name}')
        print(json.dumps({'state':'complete','baseline':baseline_summary,
                          'candidates':summaries[1:],'zip':str(RESULT_ZIP)},indent=2))
    except Exception as exc:
        fail(f'{type(exc).__name__}: {exc}')
        print(traceback.format_exc(),file=sys.stderr)
        raise

if Flask:
    app=Flask(__name__)
    @app.get('/euraud-h1-27-to-28/status')
    def web_status():
        with LOCK:return jsonify(dict(STATE,cutoff=CUTOFF,candidates=list(EXACT_CONFIGS),
                                      report_kind='frozen historical snapshot; read-only'))
    @app.get('/euraud-h1-27-to-28/results')
    def web_results():
        if STATE['state']!='complete' or not RESULT_ZIP.exists():
            return jsonify({'error':'Results not complete','status':STATE}),409
        return send_file(RESULT_ZIP,as_attachment=True,download_name=RESULT_ZIP.name)
    @app.get('/')
    def home():
        return jsonify({'service':'EUR/AUD H1 LONG frozen 27-to-28 portfolio test',
                        'status_route':'/euraud-h1-27-to-28/status',
                        'results_route':'/euraud-h1-27-to-28/results',
                        'trading_enabled':False})

if __name__=='__main__':
    if '--run-once' in sys.argv:
        run()
    else:
        if Flask is None:
            raise RuntimeError('Flask required for Railway web mode. Install flask or use --run-once.')
        threading.Thread(target=run,daemon=True,name='euraud28-research').start()
        app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),debug=False,use_reloader=False)
