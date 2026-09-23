#!/usr/bin/env python3
"""Frozen Portfolio 27 — post-stop re-entry cooldown research.

PHASE 1, always available: DESCRIPTIVE diagnostics and ACCEPTED-ONLY SHADOW
from the independently archived 3029-trade portfolio. These do not estimate
counterfactual full-portfolio returns. Frozen archive ends Sep 20, 2026.

PHASE 2, only with a COMPLETE raw-eligible-signal ledger: chronological
all-27 replay, hard 3029-identity no-cooldown gate, then the predeclared
1h/2h/4h stop-only and any-exit scenarios. No execution, OANDA orders,
strategy revisions, automated decisions, or portfolio-informed tuning.

The 3029 accepted trades CANNOT reconstruct suppressed raw signals.
Do not interpret a shadow-skipped trade list as an exact cooldown backtest.
All years in the historical archive have been researched; results are not
untouched OOS or a live return forecast.
"""
from __future__ import annotations
import base64,csv,datetime as dt,hashlib,io,json,math,os,statistics,sys,threading,traceback,zipfile,zlib
from collections import defaultdict,Counter
from pathlib import Path
try:
 from flask import Flask,jsonify,send_file
except ImportError:
 Flask=None

SNAPSHOT='2026-09-20T18:29:00Z'
BASE_COUNT=3029
SID_COUNT=27
BASE_SHA256='ee4451bd1e1ee37aaa87104aaf489b7025d3f6d7a18ca77b2ced4af55ced6ee2'
B85=''.join((
 'c-p+ZThA=Ljvn@3_Vw(Bc+hF?d>DHWOl)Un?~Ac7HVk+Y3}6_6GZzVh{P&bfDs`1wRIyuC^{%zw_1S8>9<f;zNs$l#%m4d_fBGN)@<0EdKm7F{&=~ti1AjF9C*%K`?Oz-7'
 'pa1YzfB2{W`=9@%Sq<;i|MS28^Z#5{|K`ts|Cj&eKmFtKf3f_Z{s=F#{KxWN{6hOzfB3)t`M<qC@|XV=>|g%wKmLb*|A%$qpO%$>{pWvN{}2Di|M;)}{R0P?@}z(H-<P%J'
 '|M4IGzrX5WwtoUxJ}6*bR^5BGVE*$z{M$EXd<D1rG34?NC~YNgpnQJaz(4%$fBgGDy?5{rfAe>L*WJSMVDlE%cndCVSKNY6|39@`_*Z}aw|`k+wPL4Nm3sNaU;Y-<m=_#7'
 'VXc_|79_}vT*1na`BdtjWBs9ciO}8Q+a3#j!5BrMgZUGLvmG!2d9)l2MCZy5@cj*JJNV)o@Q25az5&2LfvhyJyg{*x|KI$pqBQhk4(M%gXj}-(e}1S?`Fz@d>b*^vlnMm?'
 '0I-E0dO9zd8s9NBdXXm}T9Jo885GT9TJ4+1?tAJFn3JT{<}u^YnaAa`Xk<CZgYn#9{`dd-KYe00d&tWt^okJ<FWZTb$AA~L0Xujv_h*o|6V@by)|qgGKN%TyT7|JSd83{('
 'AZY%Cq!TpSKcReZ;&K+8ns5r<MCUz2Bxmso{PytekMI-72S5JN&@-ByP<#OxWx&t_oS<~f$KJatANdL~Xb`VL6pgeb;#Ng=Ka87iHK8LJRnTo99pfx+pnpgnEcS9pT4Hdp'
 '9P%z8htETDGv7M-XH2AozW}s?mz7uDBerluqVcTO&kkNpu&2IZ|Crn<icZd~d^0m9J4EasL3|X=5b^5ivZNXQ;GOrqzrf_JDL)8d(-YHrf8VId3X61V;$#OL|A=__N`A55'
 'Utrdn6Ms0zKc(LBO~m-vcjIF%XrKHIjc<~*!Mwkej;`<r>chFS#A;^ugZLr!?F{;E2Ma1t?Eq<H#?z`DoAGXd{H@_m&rup~T$2)#$Z?Gn;PynyhC?ajkW73ZGbQ)Lkk$xJ'
 '9yS??8udQp{FF4BCsZRGR8UrpRgfZJS~Fib7Bjt^0}sZK2hToOnmu_kC@p@JlT~>&-tWcc2f1$$i;7K$)$z;N95i3)^#(+v*HoO`5$O03XgV0HG3aoB)q)Pm7_n99#?0Ze'
 '0a1hAAAp~r(Tr{N;xg?|M-S76djqBMx0laG*?XlFZgdQH8{`<`B|h_xB0KZ8C_fs+jER-6XOt5@Bly?)ff;htK~5oDlp(`IHn2%j4-vEM1Ljrycry?2Lid>bIzrfh=-$lq'
 'S(3%fr&l|dB-J+YsO21F$lciYpc(uE_Qr;QWHI8Fl%G-)KE$sl@JH{WJ$xU+r3k+N*bV-)__Z&l9og~}>G8x(+Ba%$_4Yf(6_U<y${h=d%qn!7ZlbIYkv+M<TO!uJde<T<'
 'l}V74Dw7>>$+BS7oIb$-kSl-Q;pNX)BpWJ?nc?K&APBh>P98bJIG3`7J1!!b`g@Z<b}gl10~;t!CWX^ovtdzz(rVOeJ@zh;6Xd`AyTAXN|LxCz^B?}rKc0iPhi_e0A0FO)'
 '=hZk7zp39?E|=e|(wKD5Ng`;VL$36;3;T=%b#hKd)XI><A*C+7rX0XJ7jW@$?cy#@I*Az*UjjcKg2Dc|iiQ^tH=r}Y1qZX|>UZ#ioH*oisriV)gu^xE1mtjdZO0ztlp)NJ'
 'mG5Pg@2l2Ty~Fr>HYLLdN-4)fnNSDuNpAL<h<O8|J8zcHBFYYnIlbUnba=Zqwj8V{dx_8ybV5%E$N1^~@GU>+3&b=;{LF#VNNxz(4k6noa`yQ-UjUYm7<4^@cZXJ3^{Ikh'
 'zG%&>s5Q~lG0|VBg;!p+^6Hq&zA7H;gzi&|puEblYtmK<iPzP=*}Ux>OqCm{vPW|cW>h3&rrtStLvdXOf=W5Fd@o|xa$2v&{Bz)&QCKD5C~NY%iu6EsD&tV<a!RIx2sy3#'
 'M<Ays88u|_Ce@bup4LISA)~Va0oD5lARi?*MQz64ZsalwR!Vsgh1<4PSBMTB1#1~$39bUOpHAXrR8DE~7SBX<{cZ7|hNV&q(wcT<Fm@|d<9<cDY6A&W&~E@>xPfq%g>v4$'
 '|3<qRL-X^Qlb&jn_z7g&;@0wkFm_Stm4bm<QtGUqc33kim8Eeh0VGB>E>-3mGxq=RwadbiH=nhRJ6}+RKZ61#et#$(=-_u+|8)-Xu}EqNIUMqYE#z=|TXGfTa1h-IWYJA='
 '?Nw~wQbu%(jYROq1jnCdW@qR1_;p^(DgMxFdug<sEgH){rhl2^HS$;EkFuF`txtAe%#$Cbv@9?<n_OED+%MLI<w!#7*53@?U9kf&$~2W*ud@a_a9?ChrW7UN&_Z^yd+yfi'
 'L(uwQQ}1RYoH@wx87b=_BOE-O1~Njgjq8h~iPBTqaA?w(E?sW!E1Fu&)JvOO-^R7!Fj~xjLKw8Ha?iVJs|I%sm%#zgy0;rBO-SV0Y*f2}?z!38z+IPaJ$L3zV3R=`YI;Zx'
 'l=uyVLuucA6*L<VrPK$dVZN>UyeSaPC|D<DGKY|_kusTH*<%w$m|a_OT%45o;D3XqF)w~iuSLPU_;Rlgw|9wdILsHANzxPkB9lCk<ouA`7&DJ_8{jm_X<rQ12F|}N!esU^'
 'LsBWaPYKO^I=7X$J6w9+De)DDPO||I$9XGpe8#0(pJbv=%n)RI;>#_zx@G(K@{$dRPR^q{65n<_+~x;*{VhpYj}i%TSnp8q{+ss<LyMT?eAT-82<b+~Vf2t`2-%G*rSTOk'
 'y#G3$Y=6#>P>KHNq_*foFTd8hu)8|D#b{rkFMP;*58)45y7DSM;s0$wG-qTO7OLgdFR~>+(t-qinOaywPm}@shtfM42q-bH*~7lWmsex=hIrhn^L5|c21MiWg|>j}6Ev6y'
 't^z|j7qxO0lTcwTVDhISWGY+?Had=AEMB50L79>YskFmbaRX`9eK;dG+-2rRvFctYe>RLj3yN~B2}vlFhB}p;%O-@(sh3Kx2*D8g;6kv*)3nQ)L4h8>7tKkI=!fy+qR;Mq'
 '6BRMc`>z`+ooyjAvMv2@;Kq9NP{MbW^vR44ATo~~KxEVh(CF$lulff%=z{*THLe{3l9F$$_cIM{PtvG$Z0Ye@%+C}E<_B4&l)dc!MS-R7>hxcJ4ypl*{bzBF5uTvYiY+<d'
 '2=e0G4j=<bM0VH8IJwwbz$BlKdG!%B22!4gv29On5wA%%GPf7WZ$L<8Ei$K&WZ__aiFwS><8k+6ot<asi7nO9cneW3ea)-(_L9uF5(1J=QE;u1m1K^`3fdd_dA4Wi#EgTk'
 'm3391FuC^l&NzZn&`}P>V!5WdJ1EE~G=RahTy=ZEWfiq_8o&D+<w>fFy7XDO89S|7iJ<XXS2P1r9WEJ_R9;k8!$X*@uB2%BD43CMfLIUkTUu3#AB?WNC?k~YN5xO(?#yHX'
 '5|qjfE2UTWOfRc_ew$sCYb~0vaZ6g5t~jo!6bF3%?BMlmND%K-bs+WYnK0uBvw_nZSNpzP_hu_2`Td@$paR_n290oe^^ER|Uc@d^@ZMSzL!9%x3nJ8mw=#H8)W9%0#Ar=O'
 'jN=C9j?e;$mFQXI)<~<i*NM^cBq4Z;K!(X0F!q826<JBNkv{80f3~dp`|q!_tW+Lt0p&z9iC!WSy!QUO0?}DUGNK!vR@ZFR?JaW^hekJ`z3@F?&a0>V2+?@s2o|e|Hej4q'
 '6lnREWktydNyg8oQGKfGe*dBA=7oX@mGI@u;k^3IEyM4JDX4FR3o0jL4yQ#^iCj8(ur@j(v(+E{Y|TO4y|{cdow>5UG{n=ly`xY(5{=Kej%pCS7bq94C+}$C|A^Kt4kO?N'
 '?`Q%AI+>$P0AE)5f%6AiN~s}al64$Q)x%kl-P#i97q2S>W8GXns6euppa%s}g^TDtn7~Ay!Ozy5e2ITec=u2Lyh`^E9H}@+b&xjX#vRPz0j9zFD9!1QYrY?3Me)7v(aYNy'
 '%9C{IAzTlbC}IZuf;nr#nqb>AChIe%qFga#&XD2$n!Aic7cpZAuKVh|jXI$5A&+mGePhth>o#^neo0R<vyZrg_yzUn#MMIuq63XGQ$b#R#vjZ;j80V$#Tr=npa>V5=plHI'
 'c8Maq^A0B|)(R$4!gq(<R}0^rHlSbP5pFm@WdoPbqClUOR{d!eJzoVGRpeO(6v|rSdsS?0pZuH2ZizMD^9m;{A}X7<(?{j{q`V6K{+7;l6*f*uIxqD?*H$Fu&!QP&?r^nS'
 'UwK!7oPu`SG{de9tgCdp4Xlqkw}JVi()6~x8owyu_|eDSU?>jw^`ig>g>4Y0RX=w9787E3KN08rz#S-^ng9y#V_glmQwZ*WC_zhBa9ahuyU?fN0HvZx{<eD$CVb>|)Kwf;'
 'RNaBTbv0;hHNU#1T8lZVCi-rl?WZ#9synUow$XZK7nZ<1La>*JJ!-O<u1sMxPJAWN^r{;#cBOVWbZ=zpA(t&T@~IuX%8VeXi0)jOQVFsd!S#r|IYCmN1Ycn0A?BOO!FIAy'
 'Xfu{kJAEUPSHo$ZdnKmF)N|p;`^ji>he~5nuy|794xE&Dy|DUcheL@u$lf0c+(<VZH~CRUrzx4&4+J@<RXeV-;ur5!C198>#@oR%DzT@!8z+h#?EMtd6^2y`9fdi*d6jxD'
 '_to|U+cM}OU^^7CJDr1zFWymsSnnn;D_v&>=x75-LLb+*de+~v9FTrbQb89ryHs?n+=v%?THQ8=&+9|^-`&Fv2q?}^Y6m4BK$CE(b8+6gK1rqVRC>+(c5z(=tDWQfbBE{-'
 'h{h698}+xKpGrSaI|O7X=pfrddWap)y~oC>FpK4zt_(ji<`I1c)mZHc3Wt#6Gmu#sy(=J}1(mZE>t6#NoRZQgtIHh`(jSW%llc<y#k3{cleG2%j?va9<ojDMDh{WF?1lUF'
 'x*G2uExf~_J4*6w4i+%?$(e7#992WLDk>|gMpJew5K4nFfen#^RCH|T-$q@zZvH3;6?Gf_b3SIqZA1$NzpH)u?VhQk^2!ZJe9XI5xIKxMpQOv}P+OAIVh*SIR(J(4^>2i2'
 '7#=oiue~YTq0*^6nM2O3*+K4q=le6p8xRfXj}dnV(7X~-^PCx1h&iKBIZ)g(?=*n%*bi|>XQk_mu^RzKkni=jRk}TcmZxDx)yud|AnKo#c?8TUUkn>YYY8H7=ut_2owxj$'
 'i!|PWlW#({sXN2C5nr`IYWYFSJRe1J$+~O41Y5?d_qUKo(eN3Y2}HZde*NnVMVf49sIZ)N3q0Hce?@2V^VykksGLMw3Uf(<y!Lg|xTO(Njr7*9<F`!gOq$2GENNq@`*h3M'
 'KZtChR3EZxEyqs`_0|CyKXb`n>;48yb1Y>!TUl}97b6sBvJv@Ftkax4Q(V%Vyvn*%)sNKDy9ensW%;a_^s%La<Mv27!!SxAd-?dc?ftjZIpPS6^U(oVAN^!%B4Z*Ye5s<e'
 'uCi7~W)32&ttjnDwE>N`N3MT|6<3!L*tS!kg-l=Y!2Zb8QXizW9ymNIk$WYucOcfElsG&ou{-hpddGIdp)q)_<!wJ;&hvNt{Ze-ARf{ZTx6T1%!D6gKj^KnzCKcH+7-sJs'
 'AXu0d`36d3J)F$_HQ2yily()6RSG}InPexpAy;1zYiIndA8pz*8o>RE#@2g9@3tB+Ff{MHW>i5HGPrg$c(a2qM~wU|@`QqPA{TiT@+zL*$zNkqbYu$kqq35uJR{wHq2tP2'
 'J{@2<IN==;pEo1!empoo>$HBzX^1}@NMl?4;hB?i!$B&ujo~nDqs1I0mYLnTL(v)sO8~t52l5pKNgFK9$>U|7dRN9b2&5r5(WX~Ss!+$5<VpV<SmAeQ@UAAX*a1<>oMoqQ'
 'UF}>w9MonJuA@+XB0`wf`wf)FL-Z}JX|sW5@SDUm6YIpNWfOMNRAx+0sWetD>-FtSOo5RL;`=WdJDJ`0g?3*D-wS+|Zky9^(jm229dcwAgF_+xhWQJX4Hc<g007w?@a<sX'
 '`!ZMgQK*wT#G%f|Q=PvIb5tuj(!rE8n2}F=N*0ya{Wxds^o{3+N@prD$VDG{74Od%t`9Q0d!di9Htp-lr^754Grk?C%kSW3{oYMZ3Y7_^{E;N|@WdRF9T3fVv`%-yfyj@|'
 'L3a_eRwTN+tI*Q@HmHFQSCKb$&hLPL8ni5Gu)Rr4SBIV(VwFO}AUzRQN<Vs_1}o#RI>^YQ1HK{3yUXZpFf=E6{;brJCe0pcT-xX(1z;wO5h^*sgipJnGP)fMd|GJ`$@K~R'
 'O;$QR8PuhOmiM`T(tux17`w!v3<L&dkD8BpE<^ZHE}+;}`S!BC?MX^4iE?gyT}6CGOQJG4zJZdCnM9^d<FP=Af7k<K{XNN4utTLeb7DJo@34cs>K+h!EZ&bo=&>!PXd!ct'
 'rmAT|vd15uZ+^`Wwkr1|kHhft7D6w~yum%mK%5dZpj?(3%cQsm@<Fk%6(~UmxqmbY6iO{e1hVr10Bi`x&@@NH21;}K$F&jM3YI*!?drGH(r!464l~MqUfU{}+ndw!ql{8h'
 'a8TG8*NQzww$_thRK(atUCeyCHY!P=T%r1xY)V;;x8DPm2RS|BFhcyz&Frsd&!^;0AWO$(nOr~*30ZYoeoj+p5xWQOGYbk<0(K)2H%v8mYK7CwSmH9vtFd>Xc|(q*B2k<y'
 'c}|7YSC>}dlpuD#lD(h+otT@!@EyQU(D;-`StVu~Vm{WU+=`j>nBB-O!+HDg64f}`{#Xv^FWG@pn`xxcS(sP(_DpMel*+%Zut2aP$i0?<wsyv`aacG@8Rn?^dXgM&AdLrb'
 ';syx5=#~FDw^m_jAcH(yDzCbp)6`Nra#C;`92yt=^uhBn&p_657&ZE*Ro|n6Z=)N4`l>pZ4Q{H?G%646u*MdDkR~u*nm{f%f>PvRBy!|lma3suC+F)V8|U5R5xPQBbxkMA'
 'dn23nmC+y0s|&!I$VAcMVrlGTbwR=8mGI??n!0+sg~<;xN);DpO}R3y;f(VnS(tY(oAo&9jDvFORMzJx-u^ACWFDC6T5+hHaZKh@gp6yfXY@Mqd&5ydqPnmwxL|}D!UIvy'
 'CLf)ky&PGt++!x(9$8{L3V8@Ph#8(t%#P-)xCeGUnH0iwaZf=-S}l<;Tp{ydnR8}<Oy##unJ0gR$|!GNVdX{}yqf^en3Ie{X-+_{{VjtJ`1B&sfdLmeLaGXzy$CFrNC{sq'
 'Q_4dS4BtOjcZWpdmS;`op`PF0K2;v1lyl}BVL{Fay$jctSZ+6K0(y?sw^xA`7O8}e$BN!|3-*zlhKfUp*^4a6g_7Tl#eM-h{0_(|XundNqmZ50t=>FjuRckUoXDT0gUM+z'
 '(-#ws=P*-BTIw-(Ti5EU9}l0$m)n`{C06s37sWkcjkmyuV7lFcJqY8T=|(#oq{R#a^|lhF%VmCHMR9%)wL$8iHokDS3+2ZLz9?%vS&R(eqnT^(sh~)QKLyYO{H7OT-qqn)'
 '0co+jLuAup_opX)^2y)HX10Fn*sqwp4n7XeVn5M*HT8OZl-4dH3n{F+>h8Vx*`A}wS}6hZ&F<jWh;7mf>FCKG&dnWnI~=;RrYSG{#N1(2yhF-XlMc)ujY4rxUFGosf6?if'
 'ap+D*vC45>O?MaZ5h<&R95W^(l)_fT;jE>L_H*=r!raAFMHN&A4~GXl3XPaodnBko(XrG(kOVkL5yH+R3C1#e_9lQgWZhKOdj|@15)VQ;Gp}}Oa&B}<G9aT9ex9!^2|u$s'
 '-`?@EL!y&7uyB)ES8rxXuFuhmnFYGay86W!Yx843xxvub?lq0G&rCFs(f8ObuDyQR!O*D%f>12ILDtVt`PtXo5UbQ8S3H8C7Kwj@o=}Tk;iV{JFCUQD4l};%NZ<5NA>?%c'
 'f<fWk%d007qn4vP#P__Hjnro*hS^YQUd24ZL6SMO8MeD*o?&QZZV$U_5nHLe=fZ(kI~*G8k>@uUS;|40<-4zm4nBrxtpl=w-E?MT-2jqO=0VOb(04N}f4y(2IJ^#WGyS`P'
 '-0=SPiDQ}Y{nu?GpV~mv`!U=A3p2<3d&0!+_wPF-I(q^^=;ZQ+eI9UpH=KAwc9V@F^5o_44wmj^UF<)CxCLB2axyo2CmReU<RFwFZ>vCeS3T^6qf&2_uHgNf`ux-+MvqHe'
 'h7iyPH*XP9bp!8LrgIW5%(hhsUy(Y)WJA{ul;%a{mBL5z4>uxru=hu28xo^KEj2Z)tI(~Zx?a&)+0@_*#k}}#a?3f}ztM(ps5as20{RROlsj>UbD-UcllZ82_3Uw%!2*ht'
 'rQthN8av5r!yw8#h^PIc^dfMC&XRGEsKhSWK~Qm9Y#94>DC;U3X;jwruPsDX9ZJRJ#diSBX|fhy;mrj_%okiBb7}fytKC5m?L8hRb=*YwyaS@M@#&&Pww7`y#GZ3@SJobS'
 'JU?GvrDRaL8#cQ}vXT?Ou68CeU)_IFfdnmR8iJ;;rUw3)LCh#rk%lb|L4BKI%QR9l4@kM!nIuUWLC(|KlHa;t$_UCltzEfJybL%><e|IkA0G@tEWe|hA@r+|oDO|1QSaM7'
 'SJ(>q?v(8YgH#$qMxap4koi#5BsBFH7kT@M(>I(M6DaKn8Tqzt#SiAZq{EV5D`_a2I=<}pM4fE|Gg)O^dr#NmcRd=e>wR_CQfUq$JNX@5Y30vyi;J&rDX^=*{d`=vQLk&X'
 '#<exg%@zPoJMk`(I>6!k9Z5a+qJv;t8kUm#&L7t{#Si6-M$TWXt7vY&3wP~^Mp??f9W-?S?90{u(1O#;qeaj8w=>bs+Vc&RM&cX&?05tBSE=-Aa7UP>Gf1MYzWjPc%iLQ('
 'N;2a3jQ#L>tDVW5t*98OZquB3RuDreI(3VF2R~g{v!T+dNn_*M9k^bZ+vVQX27`2vNv?A9LRzc!PJ*p}$@qCS2U3>2`C*ZsFXR1A>A~NebLc%!1`}RivFF!<1CZhSgO_>R'
 'C8%#ZLk1cX%5``jb7&{}^hFNN5P6%<A6y*rFiD3|VX1<k(8kp54(O`_@t<vDXeUYzA$z$@G8Bfqv#8VK`;YT;GyxS9EAa>U7Bu6>v2VC22ZVRh`HGzLhDu{7Lwh^k?m!%Y'
 'xVwnP7=kr+alIBpV`=&xI+XdBx^E!L-g#aPPh^KaL^TvT7}<*n>pAg2b#R7(Ixz#wS;HXI9bR2vmQe&XV375?-&@*RmiYb?%oUbJT{js@Q>vub(4B^O{Sg|v&9S0d4+^l?'
 'RWwg1iM9Y+0~l|o?fMoog4`<@N~h8Z)hQ5Sa4}j7i*p#JgU$_<?!;-xvN{|1yr;donRf?73EIoo<e4b^M78}3d<Z@gm_#MzN_yOP?yB@^lTd+9Z3yI0KWe5_VVY=by>$U8'
 'AT0lV!9h=;jQnGnr%2KUoWzgcWt;suaNcleuEXQ%Njg#J4I|P$#8(xE)rnc8i_EK_;m36$a=HSth&WyM6qVeOzECcU_dqKTbf+p&5co_*-v&fuTVgx;r~%zfT)gS7Ft*c~'
 'hEg6hx5c)a;zqRwZFgJ#s2@TOXZ&1jeEnMo#{HZe&ybuBJ(64v7Y9?Qc9jyii;^vZy`c1V!(~)jo<}~9_TVS9{2S1$B96?Twc@Qrp*zO2TZsti$c)aXddlEu)1VtBCH^op'
 'Eo)^znS06nD-f^A#3ixHwefis@$N99pwPscBTn|6mYk&yw^_(0STJw)zn_B&HAlNI<keCj^hw@)PJiXCo6JrWOr(R~Oq_1v+g>47L-}URo|XxweSnHe@uH^Bdhta~^t|2i'
 '&i8fF7C>Gp{dn+oTIt7Lo7wf2y#fK1k;w-R3o3*B#cY;2&>0A+w4~uWXh73SoK9myLpD+w8oT^>x?~ISa7qrhKt_!I%WBa2j@ZP``Y^8I(22bvKj?~GnXa8E2`)f%r|6J='
 'tWY7J*JnjE7Gp3#=zwSIvtZ*QQ&<%!Jsu){Cn-wD3z$f<6d+!YIgVs*PKzi5@BTRQb5)d|ay)@A<;+EqUmS=Q6h;Zy%WliMit5JCpA!@X1}PE81K~k~SRQV1Whq64p>a*1'
 'B|mK;qg4HIZIZk^N-1V>h}pIZ_mdnI7+#GSNk&YHJ}fkWqcf$6>AE(of)14%A@M@FY~-BkEd+~fqe+7<e7N9yAl-d(A#xWoh-DR@poOVONcWC9IRJt(%LVM+aef7&Gc4jL'
 'w!BKGgOF3Z8yk)`IPV=tGntetn3OgKX>{15d+6{%QlIZA^E{MQ8aG@e_k@5v+C|t12D;wKmwJvX(Sy#ZMKAJ!q}9-Kux_yH=yiM`854N%%}lFNuO&Zs=2alNH<RS*@yv#P'
 '|H!_ILvwGHr!W|SXR55m`>7Z!4vp2$RB=%hvvG>*x3H(&GjaA(rGN@bnfv4!l7!#)Sm!Tii>^=7<R7q1OENO%Gp?H}xMC`1t(c{1vCqjE_d-^ci{+0Z%8=vf*@!PB<6X?Z'
 'c`KKg#gCxFpW1_rYyhD*aqA8*aGRyUD%}Q|Ko>Tyy|cL?lsK*f`4~bVrfDONZRy}p?0oo!+6_3V9_wmNPdJPca*&-^T<>_zX<dy$zx||`=ZXykr5lUwaO`FSxHhq$k&rII'
 'np-?^S~SOnIGdi>FzF7_TpuCgcc)jJi*ozr)3*GHezt0|g1Te_X0i$E9?XLq&vo^T`G8)y|KWM2l7dRnh#I!37~KrfwhDN2^cFfpLWCDJya!5<gqV6~(#76Z=*-&~N;x%J'
 'c~Qy`PV;b!bzA~_jKM|^kv8M9Gmg91?OaVlXWYO@=1tF2gTLw08o!%Ukn>m`E#!XeR#*L*$jO`f7kV8XKW#t}$>ooR1NCydfp91T?bE_oj4l!fXQM%fvyRvfmQL++VNECK'
 '4;v}FIV%Vugm(vDfJyVXLv@+~2pjJ@i#s55I;9w10rpIZF3w%r?oerVfO4mBklElr7&fD`7CbT}z673g%<6Md^B!O+X%;u&PK*AoVA!nBvZ`}+FfK9_e)cos8+rlzKrT~M'
 'kmK2@;uRL_<Sz0(Y^$A`zpHcUJ0Lo3Z!U;h(DXo#pgu{d-yltJ4I2T!e+n{cAxi;vUF8Sd0c}X#1J!|i^T2@crr%cqPGLw80q1!1v=2K50Kz4xtCOPU%tr#{mH3ZSVp{w$'
 'WftykNZkR^I4o3(YWp^iR*&C}>zsLU8lxdZ8;oPJc)`I#ZLRNk3nb)@@~T)Pe`T3M1!6S1LTeH@3c5n&D80J~aR)@HD_*uJ)L#GJb2(ITkW$W3p#9E}q1V0Fs5igIARtF~'
 'je18F?e_*!JT0lE$?dzd<OxDxlX6_>rccg{33cCqSl+TJjnK|~Varg(p@r;*ko{MBK3RlQ0co-OgAHo2htoN(%OOheCKp_Spesxx9Ix+(I|w7`_nYFHEPzYtXf!S?QJlkg'
 'K^9<R>Sxs}+oWzgTky}ENNAM753L<trk9M)tK1uKeSMN~!$GQ(zZqrO!TgzwzPh2jKFVs9Ln;-{tM2xu&x#`GG=t1x;6nqL%%MZ^#&pDnMB^2*%!#zE0zMFAvXT?2f+7(z'
 'm_t@bNL=}K3=f8|((+$}Ul-SN+t%K}AMd|e`4H3)G029R*ZjaKLto0#T4yjUPGSJ0X;&qqV&w+Bz&#n>LmVE6BQ_wqR}+GKH^eKWNH}Mt01D+N0$ri#5R{igyt00Pa8`0*'
 'ufG36Zo@$;p<^7`!`#R<WYJiJ>!aoE&(^8>E2TyS<yGe<uU8M!xoL%oaCaDVGMX~E9pwJona2ctCX}19oRrIg#i#>&2IP5X<wKAIq1*Phq}_p+3KcJ76Rs#!2GJJ!PZmU;'
 'lhh}D`@t9=-D1l<FScc_Z-0l0R01D`4lAv{d7wJM>H`N`7@+!y{hBE_AxN^XU53RGZglX!weVL=K@DGwA#CajF!9s{2uPKsH@ImJAcL|>{CK#X?%-$rRJaN6d_$tMD@~Zw'
 'qIRd8tJg%x`B9*hv=_!q)z#1~Pq>m|Xa_`-J+$%!zOYUA;3sogRym4KgFT$`XP>1Gb+$wc*bQS&C$u75ZoHj7<Xk^0-fOzHt63oZTAV_InF`h+himXhJ$@3bpKBY6`!!FC'
 '-FC(XTK{!+vRi+LN@EE#Mkjv*=M_WOSVGsjdPAuPImx`OK5^!(&w{85ATlnLbtOt&hx@5xyyJ?AFxNG-1Eo}zDBr)iHSH13YbpVvCyL0<P?)Yra7wbCj4;aitvoUWPN~>-'
 'qmR*gI~iKIQgE(@KM;TXf(&Fz;-HndU;SVwsC-`aTI$Rhmt_N@QS+6%A4vusB#-vm?8=5igE<tgjegh}ij5y~^Ujy@?{JU`GoV~%7zC<>t9e!`5Y@>O+R8W;<{RDF4<U}Q'
 'qeOsISHtZ*h&v!UB^l)-La`wC*L(zhLL#hjXpkY>)9d?m%s984lMu)(er<<c;D-%p`#si=iovn8f$K{qLQ6h3ggiJK^Qyf+@(Jy<N*Qrnv#(OsSX|qH;572FyB6%=cou&K'
 'Wt8|yXjI)+&*BDt1@EI@^xSazP18r_DmG2eYK6%11~_HeY5bZ}coW*iXlXt>9!u<|+=7*F-(u-QIO#xihA}exV9`4h<~k5}pfqkvT<67*a3rm(XEnl0__nXPjhhac-&H86'
 'XcY2iqg*N!+gU4j@WoaXsC#Jv<aA43#oIGlu^qNK1k6G^e_pk{a<c1Vp4j$>4<TE*fhn)5>)vPdxRK3JkHHSr!hX^HK6Cnj)$4^9NL05~@K3bLY(RA8LIe74HqT!`5BwDH'
 'is^Jrp3ug`=!s+vPZp2mry8}Qh)o@NSb~4T;(28k#|NZOzyQx!yr2v$GcSA;A)`~((=7fIBN3bE2&T&<GN_=$ALZmpUX9~{=$9l<HXMrAGI5kUF$JC(%uPJaWx7Sw)}_gL'
 'hUOBP8IY>N`W$;WtuwsfxcL%s5K~p6v@g#>6dYCuIaB9%-H@`%caL!k9SZ-UQa2o(EoM5r(IRO>ns!A<k<lAFYYEG9wL*6S1?jqqaN0XC9}pB2Mh7^L%#pMNja0w6CuXE~'
 '2prK*G6wEY>2xOLwS*3<vU)-Vw>a6$nFG1&+mf5kLsU?T3W+je!OJ>BQV@A*wyofBO07XLD$9@!JYm2S3{WDb(UFtrd+31|6G?sI`~vt65r2VLzTVE08qV2MTm%Yk@d4R|'
 '5a!#aDk`NZi6faO3J`Pqe8>%muBxC8t{@roc?vDW<+uz?430eDWbUD2(wILO)hy*zfk1j!|G1zw9AG(oJOB{S7KRs8s>1N}Sw5UG?G9MF$J}RO2|YN&kpg;(cG~aSP-%jR'
 'jFIK<fP0CF>yM!PB+d1S3bMPu8vjekPWH9d)lRm)$)p5C<Jedi7bU@NW%xIDu@#ijsficnF@!SN0Z*^2{PJN8U_Eh+40iT}`j~Ap`U0A%;|+d*Q&YLb%=-%;_I@svRWN}L'
 'J{pmRudMp}J7g;kopn!+iV<}_ty+CDd<HWsYr`|gI?8I&R0rKxU09w4%2OznBTJ~Uz1S714j30#fYTH@i^S9UiVRAt-9e81GyW4cSbQZn1_CU{sBzJdn-4`ZEUWwq=*XDd'
 '*#bZEf@U0=qklyuUE>!@c0L_19wOL?5e=oryy=Rpx&wvYthDzdz2PV5R0|BNL~Lb`cO|Mj@A!$#i=S(JKtS9T9{lWkp>o(*)e87hXYLTW)!hQYkdiU4qIn>Ht0!ShOw@Ly'
 '*J-eh(^sMX8p>rDI{UYR+OsuwAjh!@Ph4J{ir$Q~WPN}OPSFjP&KwS|3p2U7en{lh+o-)TER5?yTUcIwg!Zih0hOwVBaF6H2oKc$79c?<=zKi~f_mp`!ux9uDiEVmXj=PU'
 'JD}r60<LyuXB?>w$SLRy%L>8$wtD|GhKl3d4Ou@b&MM5Sq$&EfJ~BmId~b3T=Z>2SN{_#9_$q#PZ-Ts{@JhfWI3aoUn*`90PDq7el!zs^sJv>Y&lVkE);Ovgq<-yvUUj{O'
 'wTr!`ii6dV`zidoO4`oApUqvxtVrQ+k{rufaqaCr8N?;@#o(YbwCJmA@A_P=)T40iy{+QuJjG}ory?LS1n4|}<TiKpDB2A~%{6%aXc%n(NZ(NU6H!Nn;gw2*G=iKwDF4XI'
 'iK=GrSVl4Q$x!pEJERKlO04aWfKJd(n37XhX-uMX!L8s92pKJCD}xSSu_^u(J?>y!0@;6c<2_@#zXI)rm|$DoGja>{GtL0T{M$!&$qu|KK#1?@A_Gm?@9(aGbFnji<qY-U'
 '$Rs1RsO?W2Z|ugLW30R2z`>wCF&m2XR_gJff%gmz-Qmz_3YpP~L#9_IEBLR!&SI%g(w&mn(y|>eZ>u-=a@Qw0EoPRZ-<+qtne(ioP{ode>}p+g4}|O5>}vjK6s|q$s#?Yq'
 'CS%R^Bk3V~!F$T9;dZLc9S~6IG8mz_F&0j1h!1$F8Hdx!c?20-?ojDy@h%1q#L)#Z;Q~eZW&<c^M%JM-iu)yN08<ej`XwTBlaz85l~HbhWTy&CJK8YOnmTjIqgk*cp>@_y'
 'rd@ZiG-oBYWxLH5kWaZ<){x#fUSfJk<8Z@tM4v$^?ITKjBHJps)AkZ$m~q)f@QL*2if;gQ>>{geF%rXHECVpknvxQ;7&{92x5qcJ;;-34YA379-vG7gmru)S`~xRc#z8vB'
 '$Z`&xgkqnITi5E7zJTmUkk9zrE`;pVki|^>%B$Zgc=%|NiOw4j#(D@o@--XKW$t;c*puGOylNlg(5a6EJ$5J9MQ#TJ_0LxFW^oP5ulPAu_B(c~9V(4oT#3>}&o{4z(;=tM'
 'VWf!B13H8Yf`2YEZ!`fCue}kn0?ExB*P6zRL#a2&vOkddrNgODwz!g(nGE^p?X*XhF`*KEkPlNXSKlX<vte-p-apYz7B5#=pi&y!#<T4=z#5N+qQUaZdzuWfZCjise*sn='
 '$r?8v^e{83U5(gj`cK!7E&)rw9VoAY)Yq|Z2f{_xWKHmkFOTj5JF~&kIPoh;(Fx96?kBV6`4y~V8;J}sS&Oq7^$ing@efqKUz6~m@)m4ZqCsZkt({i+hGJ%@Kvwp1Y&&P{'
 't_%JQ!z&T{LuA@PO!@+$-&%imICMJF@Q*i`(f8(W9t;b=VEHshyZ+At%n>0}P)2Fj8+z<^14lNe8-?E7qnu$DV>{cD$O(iQ4$-9ahPMHTcshM*%dMb7(^uAD@hTuw@;*@+'
 'l5uz)WH7SlE|C{sUA?yhqIo-AVfPe4<6($v-)%GiW}6L>2tTGTU^Z0B9eDW+a%&nR-tV|%7^qZ~aDo`(?bk2)Ia<I`=&`D+aQ8H*9S);Xi$pfSqL7gk^ffzYJa}e1E%Jm('
 'Y5$O9&(M_!6i!Y}4rr6@j&|{}wob^2V{=2LdoNS)2f}TVYs1z1s#-Q6n%g8-if#H@<4cd&t)nX|3gxSrBIALY$6gZm2J4p|q|$6(hJuYm%%{_y?Gmq*Um+Sl?V9+OEO)qp'
 'Adr#e)hHD*A1;qvBzB(2IU8}(uw+O<i9QNF;&t^AFJ%LwG3I`qQz^fec~xD^G7;V9XQ9do_o)zC^sVx0c)|j1K*(GG+V&<t-l890f<}<_W3BGKy9cELF-rBoLABGmisvyJ'
 'f<<<YT(X69fgS7-V+ocdpQ|E%KD{Qo;^^`171|K-pa?q%cPk$^wrTwE;u5C+h&-G9@$C^kf=?iqB!)sLb3$Z8go`s6f(7T<UT`&(QL#$pvxvR81I2uwD^w=KP~C-wc0zKC'
 'n0q9Lm*)sq7z&oGipdQTKO$pbWg<U+)C)(>x++XG-QE45FA0dX<krs@p>3!%=T8`3dFE9+E&}COFfrH&mLo<Rri111#eHXJFJ(LUqyEtUImNl*04-*a^&E}j#t;nvM@Eam'
 'BLhzknX9mrdf$wUYIgvq2j~*xJ8L5cYspx`F#p;nOqaob#58_`c+mA5+KUQ-p2?<kXIKmkClZ*+P(MQ=CHg_VSzhJa=P33BC1Z}I{=FY<<{32hiD)9WW%>il63eZ5-x_YK'
 'e19~tJxXH^<FNW5ui6JLOrpNzAhrW3TgdS-$Wd9HwN6|StNG&Kj^{%5j`}30ha85G<L&GnJK3>Hn{!G>vKzosBj@#?Ni`xdc1F0gpsi-my0hLx05ri^3s-0gf3jjoni6C$'
 'Fb{^{yQdWFaOlilVxN&^o{}i=%nKRRVn#SLd$*9WR}j(UkFCeG<ed%1;9=VH-azSOj$V$5V>?vzMwzGP*Qk?urdDRbo}|^&T-y?1E}|~K0o@e`e8c!GOFd0y0MV7ot7p#Z'
 'B_^=~F<Q`)S5j8p-B;b^IU1iNeH2BXq^yPqOUKJ2L5*F?52jV$y9>NJAS*CfiP(wxz=Am5ecf76I2~Y+yE*de)Jv!Z49Z@P^ieAYjh}No?rJ|NIFxD(ZYbmPVNX{y`<5qZ'
 'r5r?eHBV&`s;Dl$pR7o9#yR1VbtA9x-DN%HNvb0IGWsLRn8P%YybiI7(=QIkY~)qjEB0zU4+QTSB3TZ+#0GqUZlm%(A$`<|<*#}58ycW1PDFG%)}{1OFYqOA+G5W6LU%vl'
 'aGo5f!qT~Y1NM}%3eUWxLGP*#Dy9>kKJ)^Fma1?Ggbg4>3uz)jm`(H49VqlBLzIM`IW3Jg130BB8xoD!p%5&CgxX0&)fYzy&}DiSLCampdE|q+pQkmx$7AT!BMNg8>Z-k;'
 'r>5di1nv2=AitAMK2hioL`$B4m@trYe)1U2n3Qj11F!?%Kcgn;QC5i=hM3(G1fm6o7V*)@w}?gJ_^bWc4T(;*(-1)`%DF$^?&BWSM;G$wl>j(-k6JLXPUuaN@)N#!Aca|Q'
 'sNTjr(?KW_gq4a4pJ39fKs3kWko79_p?6AoTCZfSIQF>yhG_z2(34;lqI{6?Td8g2%FqYfN$x|yP8eHOSG}I|bi#|;km%F|yzIQ@Rd?Q#yLHjC3-iT1<*}-|(nQ{l;)P#c'
 'g_8<idJ)DnXl4Clf!y>Be1Ua-{kLXo#lGUYisqDoB#}pi;H@<f98IOL-k?si$mOHd&#cr{2xo;RU_Ba(Y{2t=eD-gJ8*f;k+<}lcEUU4XH@wt$dBad5PA6^<5vLpz-FnsV'
 'M&Xr!1?I@QdX^cwM#TBTQHPlOv?yiO9(t;8au44~Kxs^*>&mhk6orD|EqG_FBl6)*jWY8tsxB|fN8?aYVlR%kDFgQgq*R$y9o}QwtE+nokDGjeJ(9De#2Z&H|MOmjn@L^w'
 'I7opxyTVb>68!dl_;=0DK!Qd)GHES~n8AHT)b*1W{>DL9KsF><lS9{z3BbHWoD@vJsdWDL_^BV_zgOY*5Py#ZtV+45@09PQP7k8yi%^G~X~g!Jwdw(81R4A}_a9O!OHXX`'
 '-W#5Fy+2;7(FQ;mH~C(Ln{SWHlQaV+%SZ7*GS))IUL9;(5Qa7~QFMrIiF|0PB5aRJclIPp9pxQ#2TzL$@!y)!_CVC2kz`Hr_cL_AaiCIDP9cYzt2w@h9QsM{_v%U33SK_$'
 'sQn_lshgAcbFI>fBaTi?6nhwUm&hD_Fn$T;Et5xk$w8E1fqDzFZ`jb1+~{XDsi~3P;7V!GHz`yq`fjcO-viN@6iV4#Bjq4S7_87X7+VVLVEv8gXXE>fNtKbHl}5tI@Pmd^'
 'oGk|o&KXVw7t^+e+Hu8a_?_^kQfc#G-MuVN977ur&AXXD>xBrJ${2}z2es^R7?qwkSBed0fUi1t<}(r*5bKl#pnnZiR`Ho*pV}Pv5VGwL=6J83lC*J%Hf(iFypcg+a|C^H'
 'hHj{IN-ps#y;u2(<OJ!cmGVdFc(>SKCQ!7cLj<2f{PkaU^T*MKN+)%K(Q!oZ)v<rYYf7Lzi+HUfm%J`chZ>%6&Mne679D~0A#&ho=f%Il(ukj1F5_kk0KML%=E50GjK(gf'
 'X`+L+$k%-t86GRY40kALYQGF8F^^n(Msb%NT9p3rrnbfGd->3o*^(m#^sqQHDY~8yJT_Fy9S9@nivCb<I_~8cL>Y!sPZA~;We#+zC-e&O{S#p0cAL_gj7lf-W)5|`13sNd'
 'yqFzlJTL~Ee)}um*aXfhzX>bf8F5?0A>N?F61YZhy_t99xp0T+(7QgxURjM&yWYj_0=J`)N0<>t_6Cnk@3$0%?s4d}9i8kTaN&8Ndl~B9Ky<|+JiHFgBM<y(B`cG2kP1X-'
 'Ibdn3c<v2KrQ9ErAUgQt6UuE{QivgDCkIV=)!trQVRa?i?~ghmeYCE2+{YU-(i;#>`Y2D?KpAxGl|2b=R-?>c<rX&}O3<-Ch9j@0a%p_zYHmYfRHy;ukosc=#+iQn97Jz8'
 'H1UKj{*$C0K!=hTA)d%y$@-h>-Sdp?-eBpJ1&%|u+bxJr`GsZtZy=~MC3zB_DE1(!%7q+8dmu_d4-ZL*r(BVjcq92qPIa)`m4iaYjOSIKm)ve;9Llqt%*gl=+|P`^$DuQQ'
 'O%mpS*|pU>LIn$1#_gB4)7Ry;q_q?>cddul;$K(qPnf&n$7xjCwF3ZK%)S>^UQ4j}ev;8*UJ1DRcFzgkNPFBwaP=qf{AshH(ip*Q5DEx80CQTaby4x5>+*FCP>ZG4d6ge9'
 'gZW9IdnKEhXDwv7pJn2WLkpQlRuH;#S+8XBc%x97J@9g7D$lt&ZC`>kjMbX_gXY3)vMVIFs+}}bqMMyG)C4zbOL`KhArBNB`3p{x-&3(pHxKlU$qo^p&c6v9DS8_XIRM<o'
 'QBGWXx^{r`$p*wLLsyy&?oK&4-3Zh2(I7YdxB)fEUgB<fLBrbb6DVx-@^cao%AOzJ(s3O&;MY{>1NRLXhE*cw1Cd69IMv&j2i)%shbD^m>m3$~Qa0|CVJuxMvjvxiBl}1m'
 'uQTKM`AT(>M#sHZ{mpX`_c$~j;rdyi5#~-F^M<Oj4TuhO>M0{pW17A<p}+lIRc0gEnf~_oQ&FcU2=X;%UX8aKa^Lci4*`2Qz|5=RffDlkB$aa*+FpVK<j`jC1)~xy0x_^#'
 'evuE?S(R-ziE1ZJRMzYy#U$p5+mVlHBty*5=V+>{z-QwhV!&u@`R&~FnY4k@SeTHkk$8@-!$-d9_BB7p$r}*OK^oT7qO2)sdb#9F2c5iODD?!7gQyzBi3}FTgb2w)G(;9+'
 'n)u*sf5!b6S*(wl2t$dZlVj$vj&inVH?Pdz2IG?d0%YIKK|f!iWK_C5T$VA5J7_Ya(p+S>cwOK^2qy7?OdI;p@(;@@6x%D%!v<nW@7;Zv#+FNL2-wU0Sb3EnXgydVBk3W-'
 '31q&#+AV&OtX<jVQPzt-&ugRDwPtDgZLOcy>kLY%CNNZ!erMD5K38lztVWR0$o0Xo9i(^Pz2F7}RC<!@koreZCZ9i@D{Q>;gynzlBj_i>|2-(p85EPXM_EzOKaz=CVW{XE'
 '*6JY9PsyvdkFtN`2x`cZKe%4kePscEuj!~r6uMn-Sy|Aw7X-EQHzI?N`FAul!Irfbl}_eiq$Ys3&#`~QP-;oKyZ{+7pSE^R5|X{2W0ZjHp?JRqY!5|GV-fEF@6DZ3t9vAB'
 'l2(HiJ-$S0YowIbl?LyVNPZ+{gdt*{CaP0P89@y>6VY@d_)NmvNhCMr($0Ya7de7O>%6~0Pcke;44*)f*f3kzM&a%`PkSIr&_PVK6@J^>!<m9YlV~e)j3VGBB_ZTuBRe!`'
 'ml%?nan@vmx=8&T{&xd6e$K94iaC5iqx*Yk@FnCZQU;V&9+x>T>cG|k8?q*N3vhumwZYOjQ!8O!ccevG#hyjH!v)-s7?nLqSNcxW73|X2+@jy<QC6ocO~FHp*`4O3=;ac@'
 'L^K#J*~oVGw9lB7x=u#qe#xu*3%6UQ#_e%vj-AXWBov0hM$Gg8D_uombg13orE5{U)8c7<S@b~Q11?(P_yQu53QM^KFXmtK>e<!WqU#vYSXh6f!&zO=m^3M$@=+n>Q&#;G'
 '0R*<qtz(%7@oq3N3)5-uJ-t#NpkuHm^6ItBTgVK{>%^X74=3-yNu@yhz&<cVGLCGZfMGhi$e^qezwZy`c(2C0ry%cv=w8*PW3d5^T>_p)=!w2KksMmf2$5#*dMYTLk<X+@'
 ';s)@GI@*bYX~Ut$ER{xzm@g0Z^MgRA+v%fTM*K*VdZjg2BaTDF?yOFaz(izn(Wg1^ZZZJPkb)Au6KSr~>NEFg1&3EdmhPqsWcP@QC`s!ICfQ)4KNyzV8Zj#nrNNfzBGPJn'
 'f=|4_V4axr_*+TLWF(Ha>)r5-L?>qgv4AtLp5-G_IUX1;tugHJnk}fND6ZMecIwD`3w8Ch1&kbF09?BXAk3v#>Qm2O&-_t|wg9&A3UqrHX$9idnEM^{b@kp!$nWQn4lqb%'
 'n|0OS-K9~V#7f9ss87nPL5dSD*9-8Pww@j4jt#7<L3>&9Yol&Yc0=<4QQ%put6n>kvMF8DUp4H2fXe2fG|?ol9-RYeN?7#B-W$hQjoy!$`-%y4dV)@ffh&CTfc-$H33}I$'
 'k~EJ6%4CE42Rpuj_-`;vFJnW!cTYb_Pj*ASKEZ4rnXvH_>NwO6cXt>gV(uyj$~E@eYUhHri5t;SWKS+Ig7KY(=<jkm6_!`|eK|)w*n&G9%zKH{GY+qmx|0<J2c7d5n~@LM'
 'jW7d^FgKMHcVY(m1~a0JnVwP&P_?+`kMih+1Wk)8dI~NkrfOQ=N_WNkm>bJ=?<24r6rhHsZ}Cakz=GbTe=O0gPm(c_?h8OOGAC$y8HYP`294UBpc%cIh?*gwh%PMQJCZli'
 '+i=kal<#To>t-u#fF&2@>f?SxLPn$LP19V9+Mj9)y?_ANTQ;dl%Hb7K_j<MQL$wLdxGNuWJb}FDxcd}2;{632J0wP@C_x|y+g248kqnTF!7cy7+$`reCRJvN<RNU^>OOA1'
 'hgPoQ2&zM9!`xStZT0R;?Vj+)pa(A2I`aa|-9=#)M^Nep30z?V$alPu8xp0Ud8pCcEAKgk#Q2Kdd1eUCuyf!iYfRxiXho^tEGIHa;ETPsu)aQsm73xXWxp-rwO)nCG}^Ha'
 'dmxPd8m(~${T<6(y7Gf<^u<Z3l7AUz0|#9D(H0+lJ{;P1p!y1$$f?}`J>%NuCwVpGerHr&g`4mHda@ZI&Xt{#o+CaoPk4t!r`st=up`v^4zwu`d_$r{ZAaSCC$+e^sdd8v'
 'QE9moW{@$5FK7v-4*5oQLzVU;yU}=(_oaSFAKcI|xS`e~ztPsIgBx1u)(4sq)A=%tf)}9CdCw3j4J-qgqf)4#j7qh+b*m2>;4<Fs=Bnu(4$xxm(^u5haFzhy@tZksiRb8_'
 'cBnMYEEmUDl=_<FeB!|2HW(PAX8HZ(mQSpQ+P>H<4mfzn-q_6@w6C6d(aD^ZZz>OKHP+cOhMtHnw<$%+x6{ja#d(#rR(wXvUU@tG0jU8DC{@Jl)0%Ea5@rlf-W6v|q=TP$'
 'Ta64K9w=(uaOiwjZbt-sO1#76bB6(kBca1ktznRzVqt9KM;$#GHp`*uJX-W~b@aOpXk`wv`OX6s`R19hJ0MD-^Wju!L0hTZct1Ya8jB`J^zi0kojQQvNbxY<f*tBjxWC4`'
 '!q94pMB%rso^zMOZV*|zAH2f6s0jUf5IY-yC*p{VS!oi{$|Xv9#1q_~Xv;9H8nGB)X2j+R#a73cTsR^7*2Jdil$~k8P33M}f}I$>kI0dY<4NAW9W0$S!6@fr?OM+AjG9Dc'
 'KGvoXL(FDop~D02NydRn$RxWb>*_bDwV%^Hw(X@ID2A1%r`Of{+vMt_bfJQ6hq||*?dibC3uo-sM;fJ)SfLwtTLtqxc2Z&wawu;H%pq5Y@*5JJii43GFKinI-7$5dqEOnA'
 'Ovow%rUUinZ56Q@2P$7l7^J%G%7yqy4t52i^PN6WePRZ9_EiKG5j>PxYJI8@*rt}v9FJeTi5Z9{6}Z9@!?Ja389Ze_G7?guZdi>Q)M!q5zAy4b(0me@3ECmH+vZg|)s8eq'
 '%JTLHiTo8CKxGC~vjIQW6!(f014oC8yKQJ2L_cR+zS)2s`Bf+mZ@n^`nNfHp;5d>vnWqk&gd->qo#nF#Bg{z896A`4!K7_FV_MA6yZCF1?ToY5llO*0h;}wC%b1)JKFD{i'
 'uB|4!E0D}EREq7^P%+AgQ7VhMkQuS!2x`dvnZ$XOzM7%;>)q-GLvu*3phPD-ui6I=$^0Z#N_pV2=2d(KveskOKz_vFeTUf#48iLvdWA2-F3pS&HiG5gh^`R2*N_C~kA}m&'
 'QY&;Z6Myjy$Oc3c^yX2a^7{ziP{d4>@wo1Ma6!%>F;mo4e&VKS17cKmFHfwIOf^_(AXhzImQygA;MpR3;6Sx@zFUa?#SVhfa)Xg?5b|oc|3uFZvPwn4krnJu&bh@30;;Vb'
 'SYg&eT@B+qgbSwqK$(8JkHN>PF^CCVJ0k5?K}TiawP6;mNIX<R-WJDv$ZjZSk<Sat0MavKz1TZT9-=V;-C{@V?ERQil6%26X48lVWFE0}Hs2y*0A99$)^mjE8jTH<=CJfF'
 'E#hGV>*^WTVT#H8dzK5wc_W@tIn|r`fw9>EK<`a$oh(@5%0-9BdF!ol(@vIe$1BJ>ls6#dGks}Zfcxo9HXxe!)(=al@{CM8>$s?0bIZTcg?VG?^#xVFop8}^z)ESh%ce<G'
 '6hR4i?WiCLFg?{^xgpWnmqw5+>iA4>?AyYpp`Zu#>0aR6zv1Z5K$Oan8cIO4A^jN`m#0lO7&;4rJQDM=iujC}k@aFe#cZ^$>{Kt#23mJQW?Uj_+v`ZY;~i|IcOqypz3qi*'
 '?3Y(ceMoMIcyQ~H5z_+|FuvS-C>6uX*L!}gN5e;?Y?ZfrzR<w3Wap0Lc6~BT!@-C0%LaeV+ql9ao!uKmX@1S_$4C14e0%RTMBU)}M2+SV8`50DjC3VK9t-;ZaQH)X12KP0'
 'u0CK6gPOE!2^~V26QQvpq4Y70FvBB`X((MzO4NKN>F{I)On#)wL)Bn^;3o1r#6CItBq8<(PC<dh`2Ax7hYKv=1`BiwGYAX9T9rqN%N%c<B^Ru7!Pseg7}^;oXWzkK14sUz'
 '+w9>6S^1VmnV)A}?SzkaXnIsq@`N=pB4u;J+I@uMs^Uc*D7=57o$p%RQ0dMf06AO;YvTX&UA>{cZUv(3-4AR1pyU|l)o}Yb!W|GYO3)zN@&0(c>z%5MLutq%$lVlSO^*0('
 'nA3$kqtGkuzfL!B)g$TfL9vPDYQu*|hx82d;3dR|dO$YJD%C)c38yk-e8>+(&=O7sd4%7(O7KLrVo=WS%O8z$?{x;ux7YM%6r=-817`oYp5dnh!PrJ{7A#{LJ=kfE{1!fG'
 'S^i_;wj&8X^g^=hJFhn!8tW0)G3G`Vxz+fp={mCf4?RqqlGyS}3}rsX?;c9Q|A4t~{-Q7r{-EZI(KFyJQ?PMS^MKPw&W9k!N)v{P9(wnX?OirlmuIx3AG$*8!xvf~g?a0B'
 'l^)<o*l_5C4uiZ}EW8dUZz_4~qH$)ii)^i-3tm;wX4$q4ki3EY-S%zH)rLc-D5D4`u5DECjClc2n->_&P|B<Bfm-hTBveASVqwENLrPMOiwnOtAj%gq3}482%EjYfhHSNi'
 'xK)!(DoW75kYS|U2nsF=c0O|Okbb)>>R~>_I@$ojNYk-<LDG^7v2jGmmI89GYWh2Jp;+dR>fqav0-;!_e&ytxj3TH3j}*o~!^ig_xFKXGC&1%MAsEM{$M^z+apNd+uu%(5'
 '%(Tj=beAUUXT>Nkw$qnx8+VqUL~5anBRjI5;8T!wBRgW@2^r|g8NEBcm{MtG{$r4?!0(l@enu#ZoQ!lB?Mz{0vd%5>Gb0*fKWcn-hDD1%=y>9mGv1)sGM85AG`X@IrpA*k'
 'uc`;0K1>TLuiQW!ZXiA~rl#Qd3bN#NnJDoxU+R#5icYqM<+E`p<|iC-0kj}za3j?K_XuTod6L&dmc=X&f4p4G;PhhlV)sZ{wRfjGGhie&U>E=nr-cP;y3=#C7QIK0<gU;X'
 'ud7E_ZQzu)F`C4;>*M;bi!>W|UcG^Eo^UpnWByHBPs5D<e1SfQRLyy{bLL;3#i}?I36=EODEcYODw-alYAbO<N9@TgG25d2I4|<3c2g2S;qo}Gu4OJX=FCjlH?!;jtPg?h'
 'Y$0a_MXK`?WoCu63JO1w-<&6w9Lxov1(H*WpGGHX5<fq3k|L>3QuK#xcnjGd*aOa-Ec3IEg&oRfNk;|)a?>h2LsM0ND8-zI%zuaZz9PNyNTbFsO|4#M!S>pto7giNKw|5B'
 '`VS7%wB!|)#;QX*>b2bg9d28Mxj@mF?SH}d=0$#K7dKQIy9g`Qfuujs5+U5w7E>Oin$)mfP9HA3TF9YSrPp|0OuWcCE*_+h!P9Ixa0O+Ias&IC%60?mBn`Ir%RGE$StQ61'
 '#EkRu7tfh}7MVL6(9rUl#uzcoriltDP0G0-l-0c7MEwzR7{6@6F%hOwCDX@gMrD*6kf~V9>JwC~<xyVwrlQ>1SXSechf{u}QwrS5K?bx9<icIQOUh!`-wvkx)+;F8*@VYQ'
 '9-kZN(A;0{fNd}|ka0~r3KWoOH5{hh2z)7aku@O(7o&AB-D+P^X^vAYWC5cc(Acymhk(*BdYeKea%ktv_$w~;9;AyGz;^zfkWzO6B2i#O3KnQKr(R{Jcy)0-L4~nMWz|1G'
 '7f^v1mDEA54Na?f=nj#lQjjwN*)L+ZF1q<j(TYpC2b2#fY{wxArz|5|i;U4ZXD&EpGb*psobd*d(f8+^4H#gn@Y?cvGvVNO8?XZ7)S`^gPCOMXsJ9jj8a4r(cUM@@hQVm8'
 'LssO>%ByEIz>6Et22*}G{jLVINv(eS7JRV-l*c#cRWJ_>*s4HuuP(}63(IPv>7m;joePZfs^hH8R>;<Ds{%X1WmO~<g;xXaR|n)(CplynvhGzJpoHuMdd9kX`wd=w600Hi'
 'XCm%aZ*ITYJ?Zr2%b$%0R1Ib@2M_uti?IVjD$vc$f=|$A8TGE0^_62-pQKdhe7sA#0eG5|rWcqcvarq~EawZ)j!xg9(rF3=p_4qXibFOo7kbbzcW$8i;Y3}%y^gCsiS&@e'
 '2r|NBkd=oE8lecFu094CNe?-UAfIJe@bw-TOu-{uVFH_*&f(kG3|(k<aAceA&;D8>{KeJw$ZVpm*P>JBd)h%%eg#LTquqe(1)XiWVhQiJPHcWBTz<_~R9IH~9r)4NnJWoz'
 'W)<4u&>Wo~xr_tmyn1`5LVc3Ties6aLjq*zdfQibkMDqh)yTOSztVul?l^@ela(07gO5G{gAZZ)=){&8cO>-OcrD`puElvWf4+$f_1ODk3hFA|U8J+ap?fbITH6*gNkx{2'
 'rcf6-7z>eoh<-A@%#f^7;$Gk?tE+S~6G(lK(IBRF>cs&u33Jrjd?6X{BCfvy!gQ>eL3uTP7~scVzVmX4ld@X7$}|dZ<+=*yfwywQp?NDaswhi2NU5n^)MlawT$BXC`o&@S'
 'G>N;?Ijx$g3Uu>Fy?hMOpuK<RWjVi`LID@Mf&m@6sZ3i1MN0fZHsvH-Y6<Sj&w1n>5S`Y573TNl)gX}w?zP6-0MRXh1udo}+AGustY(T|{-_&SmnH(gE!HPx`TY7(1o;4^'
 '^(=eDAR;IaO==yH4q|(A-n%{sm5AdIar}ziWJZ-Ssxgxh3PCUpel~RfSnmqMX%J^jRz!@xH~8fG%y-7YO33!mT-gfQ9!~zgHwABM3S%PjBuCi}md0qWpA~b;EHH#zZa?ya'
 'KrM99;&D5-MyTsICIm3nV<JBccBZbsVzNs3PBtdo4mZ82<hcStCFUs9^3~OFzcJx<qV))IzQ89*N$^08+|Ls_u$1y^={LK;lcWzjSejG{w_{<OEi^MCg+A37qJjJy-4#lM'
 'r2Yz!hKf!ackmrV1oUD42<0|KJlp^aR3UlQD(<cjQr}Sd8Q7UGR0vtc8&WaG(UEHW?HBf}q<yH6c<2N06NaYYmpDT#8=Bm7!za|LCb7slir_82o6LT`3Fnq`eyAhKLqk!^'
 'v;Q`%0EJhgs$jdA8AB0coMWxnfzl|sU2`##&e(Pk>Rld;Ej6nOow4$8*1RgxCSCbjTIFa~CE#a7{2gMcCM6>dXB{iZ#=rk|ST?>+r);p4dd+gGRucLRj-qXu-G^cia@1*C'
 'x-h|KvFZNDU;r1v;w2jh${^IPE~b-(9#03iU3>qN0a+b(FbAvO3g7hZ#;@kc-T~2RIe;90T47S&4W=bew9|>1N9Rpmh1<*Bwzo-hWp7)uxfZk2un(Y(VF`m$qXdANjhQnh'
 'r8RH3lN<Ob&WzvRDnkgoMEub&K}8@tou#$i!D7+|ST2LJtAlJGxCF9oPuoyuFdRxkTFjwGKydj?pdAuYqn1xijG9&&1R(3!x!^7F6`B&%Cn!d_03omP16j}&GLjzhK)K^-'
 'JrKS;y?@a7$^L;IDvheU^=?!;Ik#25zZ!UZloGU;^Fw*{iOVWi8p#!!l0Ql#$Y36WjC#oaOUT!^5M&%q4Ow(hT<f9>o#Pg8F@4LS8QFky(`BkofBT&w8u@ix8-a&I()bk&'
 '*=+(eg*ZnYw*#eoHN)`L45u>;@kNHn8y}nnZ|DjI3hLLi8O+nb2RO}7q%)M+1IB0)RTbL{UfP68`O=C5ez#S4;PTTc=liZ7MLW_Mbf-OdDcNrT9QtTn4Do_&e@3O$ndZ1{'
 'HxQ-5?8_+eDhidO<=3J!DbmlY@rm?Gew0;XMvyV{>18dZZr>Q4XLL&-G|LH6K^Y}}mbKisH={zcuko1Z9a1og-e2+suu{wO;CmJ~(6@}u7eGD|7-A0S2|Zu%4K!Jj;RXgB'
 'k-nX|b(;s4aexwXlv_T$hWvhZ7!r0ytd9UEYb|McT-h8>EQ?LdJ&W4~=~9S|;c4{E0Ww7I1*)XF8g3tuumhrTxO}O0l0aw1wxP$EN)IDbM2keuI>7n6THh974Ym*x`P$yC'
 'J!~Pb#<Nj9<Q88M0ll-)Z5g^BnPhgDNR4000^|vVW~?`hr?s3*48fVK{yasvwF9O60tW39q40u}xxMCg140*prkg2;eu6AT7h!b*>;oCD4YR@~Dj+}71N<H2AiKAry;TxQ'
 '6Dh?Jl<04l7t{LduC+2etJ?{%Fam71`ap7Sdz7ZCVw<=si65NwUS69v$~c@-&R&k-q`|LuC^>j#47bkj6wjPk25Aqa%!{WLEg-DF0^<eu6&ozwsY4)}mRL%NPl(Nf-~9=G'
 'x=S^KQrVf<wv2VYg43yvXkx&aNZCE7QG?gnnG6qfI{}bi+1N&Yg2i+Jx7Iil*k8Gm1kZi56SneB=yvAH9T1IMu*OBk31&V=O>|aB2Y|*cN0znVY5C`pEzqF=?raMnv;!nw'
 'Y?Q`A>|Edlz2R7XGrL(Acmt*W4nTMX3eErN27IE2Mnf1ZCtkeSEb6uao#K-)=N`+Hz)l!=`LyR95~Gv);8rXTsXOQJgGBHf&^knC98bH)8I|%3Xnccq1F=`0ad{0-Y#o>('
 ';vhHf#4?Qobp4eRb*>u@-OGuCN~yS3cHmhJ4_RAVw-LZk=G!wS<?-48?7)AbupvKb9<pvy*fm!K-(ntXi9R7P(`b<&rNNve31ZcMmh;(#^#%5$z#t`J93oy<>E3x}^+~E4'
 'z#>QJAC6^}@1DW7!=X4D%V)z-Gvb5gxSz^6)DgMnFRuzki`V88D-fl?gXoKtNp$B;XU-Z#MzF{=2AZ&c1r_R~4)R5MS$)E0mq!^rX8D`}*P|V1BTpOr5=93>2R71`DzD{F'
 'C}WX?W~j7kdwi?c2c8uOsO%!jsk*%SfcIKpIHj27CWW#Z9zm>ZO~@YwS;HHWfPP?Ut&3ptuV7)8kEMW8DmKYnkDn8vgg3O@`P<+i5b%|+C@Lu+%Zw)wUeUaY4|pyc4$T`1'
 'qf(tRR3|9PMv~;${7{%T@1U;QDU+704Bz+8)T^t}J||0!5QEGTmR6~!?l&Bwpyc7tAB|$sXkJC5#XOVyfe%jLp=xJFMCo-z_`)flC{?IHbnhds7oH+$cW5Z?56(dJ^-mqF'
 'IVpEM))<^_0YwdO+K=zla8zDU<d1@E2K(cME<g4+j#?e!FhWd5n(ZvGJ4OmL*Y;d6so>)pI7w0dx_ZtuV~q8w5ZW=N0X@@+m4b?t8?Yi%Z&|hDvZ!mU%LYV|=PaLfvedma'
 '&}mP*v7Gi1%WqFt?LbA4oc>CKSnDdC5r@nUL)zgmDyc&p>3Xhy9Ad^-Fwr8F-yZprEktdC3r7j^vibx~TX_`d-q6gOC#gy%_q%;|M#Z616=XRW%B%K)L}F$KR_n$fmA|gS'
 '17|0y6ZvTX(6RQiK}>KkF}IaHRUn`eGl4*$N$ac=lIXwHH0vrUClu#VLxyy0UQh$RjEZGL0Z|E<gtMitVz)~5LW8lxacVHAGX$;0><`I{{$NkQXb8~;;}{XA-o%QDlp>c}'
 '%4r+mb6!emEHi>nbb}VmRfRzno>JREDmJUD_GT+sAA~x@h(c+>wt6<VVR1_$?EbzZh~MhtDypE|fK<J@t)7hs#Fur^!$wB9BO>~3cJ>a{sZC!$OJ?*H5S(7z(~BIl^?`uU'
 '8nAP>_!X4$8z8B!udar>=k(MkISphiY1lsGylNkSj3r5{hTN?=tgCTcrH!w(ZVwq!hn$IqCY_fZEK(d(AG3m+Eu^WeW?Vg%jwvw4lV5+&jkwuC(!mE5+RpMSP2OXvF#{eN'
 'nZ}sy5VJi^jnm7yuvo@4Fa|ByIF%=_Pc=HFhaf~kb(J40Psoq-nv0<P(Ky213^%(e1G<Un8_Z5PO!Mkz+1}L*XFDLE(s6MlQeE}8hZx!!Ncp2d227<V@T|)`arni>8ZQ3?'
 'yA9=;I*TwUH$XCTVqUeH+gcl~Pqfb_@ZvVw>t0e)@+nrVX1J5n5#{Jp;(YlU7?&;C*_)Pa2b#<aZ4*RfZAuq>cz4|=y@#f#q@K^pz9W!Vd3?V$Kj3bMLv#95%dOdBX6cUn'
 'P(&W;pTvGPX>;;ZQ7Q5u6w7U^oeADo-&E{?7?sR(!dcQ2?2do9w)u7kL#HOVGT%#bZu;qN$|~-F7&U0wo+I{J<F5~g`j`cMMkj|uRIX6(Zo%3CQ7T7}!%l8%o^LeO7?fcr'
 'l~OoSpJT5%_!hg-)Z`82OcI@GmNT5*D|b6>e*jG@gQ?~m|A5tkwmqWz8zh~9uog54eLi(Hj$gyY;|n1WM+<Bnyen&ePMvR{G_NEluLVioK<u$<Twc|*L!v`H$}_WuCg~D`'
 '&H6-hp3K5B<Srdp%j^^!ZGYI1y<!8Xsr8^T7i3_((`tOcc(d|~Gk-K51`9WkQ;PR9y15~o-+&mEZj%}5Hktd!*VPB9RND@-$?K|~i{f|0R-a6^E!$Cxm`)MXD^*+Z67#Zg'
 'iTK-Jf**nsKMIv7)EQWE$2E9;j#l7a;LqAtJGtYg`X`3B`Lhf>f7IKSe0T6(+O|ii-9qdI2{-0qZA$_>6#pRlT~|BNLcd^aQr_y|HxH(Wf@hND4P(sL_=6h|(t_^S77mo;'
 '>RhaCO9n=offAE&U40@X%TF>o$jy!F0kRXidGCPS-20wGmUNk*7be`I%NdA9mt9M3)5_UNjJuZ}>#k)#lg0eOZr?%dPV@4KCkQxt8`#=-oxRTJoN^;xPW-vjAEV^R{E^ya'
 '2ScYVfC7tZU4{E|m97L*n&92Fsn`~BxPP>Dh2eF20a5l_ozR1HHFs(S60~BDatWkcFM|0w&|2L<BOl~$&4~NkJ?xCa>Qtk-=lzuOnQA2JZbuLeEa41CYyBKOU{8$04Zv^%'
 'UH+CE-o|F${SfhSLTiiI^{NG$luqiWPgLXSA7|qH#m9z9Q##;Fi%~x=@N3SM#`O*<eOc#VFqwe!=@Q>|K8?2!<*>{P#j}^PcUKguSj?}(>!JuaP0<Ty&5fx`Jy?tk;QgD%'
 '_~)D83`%Fu{o3`z@&>+P<C@4_`qJm5k~!i?Nf5s{>8Btk9H53gkaDW)HK&EixV@Om?}&EtILer`61VBVAqoEKK9ITD9o%5(jA2gFN|4DMJf1#5IvX47|JYuD|CqrZ;%`_Y'
 '4)`;*^Eu5Qdt*aDT8&GmvGLm|IioT<ogt93nxP#7cAF_DP-ZprN23fFBpL&?lc<-mE_0lvC}jfkYSh+eOce2MI5bE6%H@ql#vBfuoHyQ4Zb)>f!OHj9p)Hpb3Z`rswqQLI'
 '+sV9OhQvBOfhea^!s;Z)^Oh>0VoAWltY;odO{@w%4(`xGTQGZ9>MB17m5ALCvA>_9u)@$H4l?4*DtoFShz@zt>{QB2yV}tS{6UF7uJ1Ak$9-PK`}tgUNHm9hEIo7*YFK0F'
 '+tX|t3?*bIC%->vfQ7*`0Tw;M#9JM48!9%1-)ZyUu^n3Vsp$GaZa=#v8HLqpc6y0f)DiTjiF<tcfX)OLt+QzSyHxl0^xFnYr!!5N!+r~&gp+rf!wf@nU{;RVu9H$$`$P-X'
 '7lfBLp$;id)6nPmpQ*!qE6com*2_D<11FPE%VtwIK>kgH)Agklm2w9xrdH}I-|gI1{ZwRA(r4wXp1g|Y_N$%>gH&P$Bhckhp%XRdzM&14T>iEEGx??#Ojh@%f)T^avU>aI'
 'ocbhCLq?JzcLU9vAS3D^qkK!BS38H$)fT+K@JhruI&-kwmDBhsX*vRfHIa#8jFWy!!Nf{Sg2zD|9sKH2^@*zQ3IueZt(dgRs}E4%S0F~GC}f1GP_)uux&zVSZ9Og(h~{hr'
 'nJv7o#+wKE?O=QZnPtfP=C#|(cPkK5fzBpUk~4tR5pZ!>S&`^OJ*ZCqWJ$ftT7$YAJ$*Eclp{xFbiY1l7)sTM2jbEeay&$BJ=LIBFgDHoV(T47GSgRo6_rj+5XE+3*tm3_'
 'QSRkj*L4V|&sy2x%d7l^a-&uHd$<N$(y5qN{rz*M>a$d28-+O3$*B4CmE;$h-Jgw#=vNZI`dF*ofEDjt6SrPBanh#j_v0RepUl}8Or+GBA$()+=OFwVu^Z~SJI&M6%aLa`'
 '-iE~<MkJoh+80npjb98Ia1HQg*tNd^tjkhi@jpbsAXODy$~;hT1m#-^G9zJHb*GGk%Uc;ev0=29AVfdgV+kL0N8=4Rv9c|%p3xIt^IkT2?2($FTo;yB?V(fiwtDpnL?`Bk'
 'vaJD~SC#YCf9Jj{Px4CLrsJ&C7W1(a`NsFLnG!5v=8a6$_B?McY@28N+5B?Rcm&6(gRTzb{m~8IVInpDuA9Oe@2I)AM`wrl>}{+fH8Nm-|GB<B$V4ULAk50Bt1ymquJB)X'
 'NOXz~()B@JwLPwfj|-4Row?ked6>Kasi+i3WyJ`h+`*m_!_PBk?{FxkKG1GlUq}Chc7tBK^N>pt^rTO@dTr`q2Ln`!fnIP_^J+Nlzol0y(r{=USnt3y&bX}=6DuVi1g`SB'
 '+COr3H`;Y9?eg1hH%>oaXzx&IzJNh&mRH?$$F>TyHaP2nIzYB9-I?Hb2ZI<m(UpQXZt~E~khJK%Y|OVQA9r6YR3Jfz8H`YL$Auu)s4MG#zF4S0oa)TMwt%Mv9TY?9oiz>@'
 'tCIc+I2XcKOrVr`6jBg%_4aOJ?$D*4K<@Jh^Tw>|ZY~+v!O-~J8I&w#G+&W4PB!Cmhkte?ZUljdAg}Uio7a3*$qt87-YhWM<yEhkEJd;Qj=1RJ<Z+)Nd8Ncx_feF%>$!w&'
 'C&s>l&3QZPZf&46;-{8D=CA=84EEaFL}MXfM7R8^oczDrL@J{@6!NDsi>e<+(0X=;+tWWgAVwu~kT`Mb>fJ0v^+BK$Gq8NzQ;GX-k9_ijoE~u)AqMk^j8z23yfM3i5IN*k'
 '*Hz_9&&mykCR8A~a%x)zc*1ifW$hGijG9rj7$joe#R?XUC17Ksa|p)HCFyslH0Fa_^LW?+3zT3N&poP70+s8{EpJwfIZEu^HK?Rl3^oQJ6Bqz->-pzbH~p>B8!q`u{T5i>'
 'Lg?{NUjBY^o3$Q7Zt6B0$YAatw_G2j@?ZyanR(UqhLE-H2qMAakoai5@pu7zP`V@BW?nmqh|>{AwCK3Ckq`}0_^5>aLCG^HQrVW14Y<>ed47R|2<R<1?}LZwaN^4igptZ4'
 'DmGxiI^?+ixL)DXcWw)}{5&HC5+_XYn@sVnvwJpx;Bo`0u`za-E|fIpavNjy*}{)}VCN<Er&D$5O;`}gyZHzrKNyul2WeJgUA?_AV0#iooz#JZlB;bM@PX`A#$lC^qa5{Y'
 't9LWk*GD;}Y9I=9`+1e5tHBFv3wAg(M%%V?3p$wL_IqyI&Q=&{1`gl9KHxjvKh(c}4X6&}IKG&=fZMD-%4n6D<c$5gs*mF5<V?yeeFim#?9N(R!J7J$e35H50NM@s!}rr}'
 '!1okIFd#F**#<;uL89z8xwW#E$C(KGqR3%!x{XV1p*+(CT@h)mNt7KDSLR8a5h%F@zO_Cqu@4zLef^q2siGbyH?wl#xy19J!4syb5^RV=q*<Tab-Q;Y_)fNrc@^&-;?8gS'
 'SdC$H#hdD9v9{IwPkGmRbwkX^Lf3X(4NtgJ8xSREJJRj`jDuFU*)k;FL>I|moY!r%YTZUJXJ}XW=7H$UwX85B#AHUAO{S;WH{9rKkJ4y~Uwcoaq_QtzJKdg=-EgQe_qjLf'
 's-5TvPz!nkqPcPK?U<hybnH=5Hu*s|>>b4Qw@0$GDcKE_QtDCY$mK!;*tto&!42Pl=s<&$1K|&@#h!QNUNMpnAwUd^*ypc&GA7ks;7Y$Knv}fSkK=cQC(#98y>f&weROW9'
 'lwUxQ<Dhl5^98hY@vwXx7xA0LK{Rk(g-S*T%CF*wja+__{1xKNrXcD*-`{UsVUQBBl~drErxPAQY?WDWzuta2@hHknX>hwYyW!BCL>T3|CEt$Gx-B)4pQLiFg3O_vS8cDW'
 'yE!~9$NG1PIA_n!pe4}W5Q=s@ubvF{#h06pA2u9H%!AV8yz1|Fz=OUVIkW}GEn+X-VP2Vx4Ta@N2RIxO3=QDKEdlqp`tzG~RTIGnxxY29(&=ROOYTEL>2FmD8p24I`{OZg'
 'DEA4ipD@H6N7{_X7_aX+5u>xd{I*C}2tBH`JIy4KN~l9A4t_?vGZIpny-a!`iXERrjq8MA-8-4vg+<ivfe~XBhf+Ooa7P*1wD_IxBt~UC7FVJ~$@Im-8FDgUmX~P888Tpv'
 'XPBHXTR{aSew2flC=}pbuO_kOUKv3Kl)cpt-q`V>&c|DY?;ryzz13KTck~TkUOl16#Md@?#Iawr#q)f_(FRLn1><VxoGA5_?D3~_4iH^1i@K9fWsm*>wMnR~?mNgEAZY_A'
 '@j&f1V4Sbrj2?`icfdDTnm8?%CRa&~(sw}mTZf-hvl|YD-c%0WuHF=W+w*i!pkRz!q`tfZ)?b$HDd;r%lc7r9S6ZdhV(a->qM$$}U=s7o1@LM8onPecEd(E3jDWKjgB6s~'
 'sg4t4<gyyh?vl|ij&6wT$pveSpG~Y(RJt9c-V9m!!Ac&@rTxbRhf>Yc`DtXxP8tAwWodZjGgi2|iA0nIGUvKV+BvU3SDIEBI;)6&B;|gm;Kie{wkN6h7ZU9$jNZmPmiK0&'
 'pB)a(`v;>whe8W#ABZRb+C-Er=apbWiYPLjp^4{?*5aS=;wgsUiFn%4DyYIY8|W(vv@h#zm$9Fc9BN8_Aw|%>H><s62}S1sqoKvkg<jpyH~dgLQ0fgx@qQw!gWedgR&Br;'
 'X73$9WD~E@8m|lfGeW6uHWOkBqhELU(>xoKiGXadZZ^a#pn?*=T$2i!8i7yc@0@jh3GV!&Nd~6nk6mv|#xIpnWIoRh*-P^RM8`{jl)nTQr}clsq==0&|D||5Z3HaCDzZgU'
 'OsL)Wj;6~#D=5tij4fwYy8*G^YB!<OvI`LX;;x59f82b|5mq7<RQ2e6f^4@SCh!h6h<qe4#2kzbljJo86X{-o<PV_y3cD{T>ZYttLpd`6`J8+fU%c3yXaFhwhcP(qRerUB'
 '+<m~pYnuzuZ?871I7s!THjUK{<~&c6?~XU}gA_M(w(uUBu65s5JB*htdt`w6r}~t?<^p!0K(_%W6o1y$c=O$Jeb9N%*S`59MUnd+E8xWhHai?jDf`0=krs0}HQ@7aqwTP3'
 'o?`a0W_*YFmt#gmEGS004`&fD(CHv2d{K;y7MJMGF3vj7ne6&lQseLEE^Hyr?08yjTXKkzqCrl-MlBL*jR8{Si|oe8Irsvc?$BI(-6h_0aq$*6F5S6rK-KlD*?{d0(Y(PJ'
 'euFrU3`<^BCy##g$ajy@Se6_;NtQ*+lW%=w>&Y!)DNgc9RZJ>7gEF^DUA>zywLS<`Z)OfNr85)%f8MSJ$#Ip}?x+h%fMEX{Yhy5R2*OG*?wOl;ugF(ERtq8Yb5v#zt*+xg'
 '0Ra_gkZ8y1svC!&>5t4ok`V(kx;tJ0NY#U%zw6(|l(nzGQbX;TMqpF%xLqV1yNJ%yEwA9;IM2qf5QMPlX?1ZIS$$H_iap9FDU~5G7x(4pTp&Xs_=q-yx!dOfN|6$92c4rV'
 'b2L&V#+~8y3M8m8_fvB2puW2{>d`wBP5x_Rn78U^H2JywRcS6qUA@R0C7l5_*6n}1;c4#+yDiz0(xcR(T36%hilh1@qm;Q9Y8Z~yt4@zZh2%V%2+k0?d3P~ma%%W6l(^8w'
 'f3ohDj5~fTOtFyHhpL26ZTDVh!(MberLNWJ18nyt?>8gq++=9C!m=tmPi=Ujc?HG%AO3@F&mP3E9PGT&q&B27{2h3Kd;hd*$E}Lqy6v)O*hGW?%u^)-r^kZ!H_#_<c7-Rh'
 'Gw=ECp_V;KjzLEurI$e)MJ9M3&Sh;(S*6soJ#9BLD%7b>W>%-c3w$vf2!Hj-4jej78AUfTqv#XI6SZ-mk(fvJJ+FpKH<B>23k(G0juG+`ZX|tU;ZfqU-&d(S&#g_JT6PZ0'
 'rz5`M7l0}@d$;pg!W-x3+Y8uIjeSwxyWN@$d9Ze~?nH62y7z=iqxG5pR61mLo=(G;v@Mn?%@MH3b~EsERcjTLRsIHoz{gQn!|JZf6Aq0dyfek35oHeMvIX#l!1ay-qE5)-'
 'pp3j~d;LeP8v_P>w3{D{+fIdfH^!E!8R{1ZqLzTU+^4ssWIlekLoE?YNL`^5g|Ip)U2!<2nB7pZMG~T0!_!$dTFv(Ip>NyTw?5^TCD^RD@yyYCp5ol9V$!1@$(z68;h2!O'
 '*UOvbkZESN#nt7n6$k1dqmdKPwhiD2S0F$J&z_9gTrJD<B1fZ4M}N=-xA<v}B1a7vm0n@qbzXF;&|VI_yeAiM0MYqTIP^m@58Xu|onUjHj=${qmRI<~Ex{n?)Av_cPP0{E'
 'D3A4qHljgHMlp8BqDPF*1c=^sCP;p*=TVJbBni7fe=;l_(NQABA%b4v>zAPwtK|!;-<72A_6pi-!x*eV&OZUeyz-FNCv>HOw=ez-->jZ9b;6+$`A!f%61jU~R!Ie-mGi(!'
 'aqZl|Rc0lVxJM;omh<PXjMKe5@p;>kmK?k71Na8~tRcVMznbF8vX%g<s&So70U-)xt)t1yW`?BHmu8r4i$3<u%12P>#XW3rIwX`a872HM+M=6Ffp~dWwjo5`T*ZLo$=|VG'
 'A*lB&MA_fX@`rImWpm6^rRa7ZIfGb}Mib$cgQs4Tba5@6Nc$TwZ^Q1^P~o0%=nVTPH=0-Z8N;qlj^?jQwIlm#$K_pdfMy(|lXH+G&R*aPy+6pGaVV{M@`v3Jvc0tC&YRRC'
 '!f7DPfW1I#%`qtBvt#t`5gxzq5PNx;WL<Ta^0vW2YDEixSb5lg7PwaqlXM4XILq+u6I?p=QC^2Rg+*l*haj~8-i{gG>Ax;8gAuG>Ud83ZqfbCI=FgWYU?OPOyYnmvtsFp<'
 'piwTO@a+K5Wi~tG2ukH(fgpKb%`~Xb$eC3H$r+ZHi_3kopor^DwQsAcMq(x-NAL40ufJOQcHWtcnA@nx8)9y|ju)l=@^h>bFw2{7Uq$nTY0f}`8nc)=<B}6zuE-gNQzDLU'
 '{C*8$FmI!<S1}EqkZ24u57WXd^fNU##myTHWWfLhwDHyT`*ZOC>(j!#A9(U^1pmbuKBFj2SI-dfz(#YSbyIG~pHq|>1+4{qufh8UFpqQsO|N8gg-)<qCvb3K;dW}tvO)oF'
 'hlULy4^I{3-p0HA;O2I)jEs0dC|y^d2pniBrInDM(2K7i1HJ|48pzqzr*T4$xDGYl?%!eFi9S$i&cys(6ovlOW#z!E6FR-UUJIGt(q!u{D+drwzCE<XEiGuiEFHZ|&a5oN'
 '*a^SIC`Tx9=?7ft48-a%kK)on=pYfrJjvTx0T~^3k+;shsFH_Hbt-M8Ixz>ZKaub&2Rmq4>@vn6J5PDlFE8DRN+O;7)i@M+RNM$GMjIG7eIiAkP97bolp?=;1HXL2>@T1p'
 'h<XC1*G}4uYqdntBYF$YMCQ4fE)^8()TW7+e&W9#H*1k;JMSX`c;s*c{`6Q-;^(3KyFro<`G*a-vle`;?K*S#SuKAAxkNCpy7hOS{2-%5jB;aUUX4$lOIkki`c;;(+uo$v'
 '1%AMX5Qmrt#pqEp@sIuzYf5BCCU}c(nh_&oa%%VpqE&B(d@YQxef)ZdW_^@9jY5Io-NBxyPo2hgT=W@m;?LGF*g^!LG^ob#_P--{>mH3tUws#@k+OSvl~#{aKH(shx-|KY'
 'R{GHEzPLNomQiSo%7|G1P#1mr=J%6QO|755D$VoAt5KT5dE)9cCIY9Xk%*qd99ilKlR`%_^ZY>Mh$<3{Kt4L}aC;3qZO98KrTkNekzmLRcl-lm(oRT>PU>Df@sre5mEN9G'
 'k@t3fI!Bf!E4>RQuc@fZ2#vtbR94l)<!zr(N-6ZFv~LVr&OD^mZa??U=CGh3oo=US^k8rDBm5}kNviD#J1LnE$4{&AnPQ^?Bq%ZOB~T<`ekUSZMRi$mXmoj(q~w05aR%LD'
 'qq}uEqi{MQ1IU{XG$rV_08S`K37Cd}W6#9hzxkl$lp8{pL*QvOd~vCn+kDy>coC$$`LNrF-O>>o4x9T#jA?=|P4X00OuCJjbDVwu19T)@!38uSI)_M++-w1K1*LIC217=U'
 ')hF^(>ailOOg<SHKQdpivrpI(bZX;=G68f3Y|MHB=+Dt61HHh~@75PG3Wf0>)VRT82x*uIJLBAJn|7IAqN%LQUzIqM#20AFwmH6jyos}^?*a;y_^o^Z<zWWq`WFxZ5Rz3y'
 '?~J|U7hsjg5soSaKc_KB<BJ^E|A?kLw+gCo*9(Lv&eQKO%TzLH978v!d^?{)#sSKW+6=#IA-l^duJjj8FsV7tVcd>Tx?!V&Q;U31(Uw>7lDn0HkCaB{kl%jHWP4@7q}-<I'
 'hz}xs3>pq}<1OryTw_EZ`3C(-s95;|@r|>uc>%g^JD;9Yc!HsEx^VYVWR-8GiY)SAGKy;*_xn%-E420<tJjBVotK+FG0u+6J;Az+bXJgEf50E(JXV(&Rv4%hx);Vn_|5>5'
 '#W~ITNK#`TOB+Z}!}cRPYkic0{Jeg5AaH(4MO4Q7z=S9x4wosO6iD7nJW{<!a<nE7&x#6^FAzpA0MMiB6OH$$$iGpPkU;Ac1?bK0w5aJVrm-euc)|fX%tQMJ?u)|vDdhmY'
 '0YnrU_v@<DD#y>YS0^Awb<X5!4m$w-1-;2EXjUGMO``7;wBCpMHS=1Zq%?i#gei%2m7alY-{K5I$bJafU)B=ZKQu_eM3+AKb<>%$e4DH^gB|mCIeD&<<wNe#XWv~LegXno'
 '&^VBL>`i1{qAIMp6!$>AL6(9ix0d=;6e``u)~{^=+ureVbKt>uMgt8Y?-AU-rOvB<c^3NwL}RJ3&ABz8!93!lW*Al{W&k0@bF8kXg*oBSSPYa?X~*hSn**g$6Bwe2o2~Zd'
 '#kW6y4?|^s&A%o#4PHdwuKlwe`4Hy_aBg0dHYL8#fnImO96)rN5~DCP-tTh~;F6&<<r=oTSMY%V*@eJ&*cFVbsqqI$@xaLwsNhoLy?e2&LE!sTKEqR;L?6?#I&Xpxoc7cv'
 'pe`WyD>d_~hGOU(LUuOJ1P3c5uS8D*nfd_z-OM*9AWD6=LW@IPT~8`pA7xbDHw|N$Bsf`(o&*OFjb*=m0+XG-(UU%;NhLics!Dt-=3I9<rZ!JDlsZ`X45gj9jh5#DE0CZQ'
 'bVI@3U<U5h#V*;C%|OYqpjlRL+j8Bm@@+}U1@Dh3K(gUR=ncYXlV_(!JN7hv1<Ih5raOnDr2Uxb_t*R8)P}`|T0$^7ZV28Avc{<eJCRyAfM`JP$}3)vt#H@8+Yh%bA+)R+'
 'd5DV)I0uty+``BN&XUJe02C{4?Tp>duyj#++nUSv72Fs6^%IU@#=%OdOH%`m)hj-}&F+O5!A0*kkI;E^^A`!*au^S#j(#L{48366{hHn|<jOL4yVofBB>O5vef_{hAof6_'
 '5pyP7m&EM$@C-iTlzdRok0cE!pYM6quPuA1&p|z4mI0HLUcFP7KLj%b9E4(?x*8uad9L+~Mv#%@>-xS57rpZKkJv&WW4t5B_$r~E+Kih^qRfi8(u+gVJ8<u?yx)yFaDWyw'
 'f-Lv<iK7curAf|=RE0o}edksGgy)wZWmUT2+d#@2=$**fxb>Hs4=Qt6%)T5R{R1(ts>y0++z#KKVnZ<l3-#)C6(7hy`8MD_lJjQU&<Iu|ufh|R0tXOKsdI)k0!?EQCHv=j'
 'Gu9R<S_9VcZ1?R6mhu%?HX}Q)vGJ>0UMdc&R27zu2n+QYcY97Bf*S&k0#Den`oLhK3dHCzlacS}nIjq3vTIcopa;w{VE4t4ke}UCuOxUlzKXI|q}3-<Gx<q+r9c`f#Z?di'
 'H}}-kzpfg*=@(GlVL06$M44w&X1e5}0=5n}qYWW2;xzsY$|&($`FhU6tl@?$Apj-<nXnx>K7yO7^*>?KI41jd#nQTc6&3Q|)Fwn?hyj4?9lrppEVSb`Hv9GhVed7!m#p4w'
 'B4bK$8}#s+f6`<1R{a7Hm6?z+d~U}xE^`dOgFJ9hcz00D@`H%=?g_VbVxKbB7ufxN$H@;;S;bIlG9;xocsX*N{xE=ZL@^l05!g+-SE2PVF5rt5jS~F(UDymcDj~bKa^x1W'
 '>vj6;U0C&1;Q*q!DulMlvjyFCW82f;Iv`B#s~$HaIBmN#DAozRVSR3GH(vJO-lQjhN-sRf+(`Q>nafO>+s;c9xAW4ZfBixE{j-M~3N?}zeSe_RzKY?pz+v-NVj!mc!6D9i'
 'D+eY`C0nLbmJj<(q4YrAKxl^vUV%Qt>VWo7)C`7}VrmRJ%5m@T-E{3KYm#)5bjmFbc&=OXlFpI5-EKgEHk0VcM!Lba=teyEAhs_6Psv=}c^l;cQF&F2{B&a8Kl|HJIUuS+'
 'Vi~Zz%;%(%82|bck1^&G5|8?F#<<U?mMV}l`MJ|Feov*K4kQ}GopIq1HBH<Zz5yMyr%fh!CQ(-qE)B{p?n!<U>a=6jlbBcWk%rayMN?wj6N3ZP{J;q06AlIDL}O~j$Xg)~'
 'HtUUb!J$!(0?G~{^D~g0u3El)HH;vGc?>cuA@63ftITzk%wlD(Pf{u~i{*5MvDZJy<xl{DPTn9l2bb05Wjp0LN}X;-=QMyZTt-J|b3wKXSaggK(QW_D9x|1yR`&uRn(MOa'
 'AJOoPL#N@ni&(_mWnx}%Fjgc=)Lv`@`J@b9#fng#q*RVv=hBA^yQj{V^T>f+!2F4wnP}hc&89F%Mh|;z(#q&bafbu~3^sz_{#Rh^eO57}BlU|&p~12mm&d%zgII}}#Mn+D'
 'I`$9fx|W++1DP*<z%*uriH#xS0szDtB>n|_9_mQA3?JX(TtPeAaR@mIgD&bS=m%YxA5WeGhT<0a391A6i!0fWVb)K{4IwuTSPkS<ZV%(`3i=D1Vxuje^XQ@D4@&59cmnlH'
 'c0Z^|Jx2pb>HjbWXA`UimFf_}oncFey_VH*-L~aI<ot>AZCm-Vs>;EF5M}(|u!BrgR}~<j#LV(8#cyvwKfB@B9vT;r!3nz*OZM+-`mE(mKg!=oJRM=HuryosLl>G?=vP17'
 'Uou^QsCf8_WC!XUzsTqOeg+-CU>)Q{)9*#d(4FD-$<}P6VVnN?PSE>%ivpy!h=X3aybAn4cD?{%t<dco2UM%v*h{uHLXX~hBIAPzu<cczPF+@5y8VfAdIzwb<Sv9N%yuDi'
 '9HQMEUBnwqBr3B##e(1u_T}C?1-2E%0%Sf|OuGlszM2@6Ru?N(7)FN}c7KZ`#3&8PIHlC0b#^<0HscZjE!xSpffKCDLYn+tsVg+E@?{n0JwgXnTWJ1j5F)n6>Q#5fe)V(_'
 'HUne9o_6L0{GbWi?q5Zzm_4u3+laV+)h6MY&!;nk_B%JA!cv^62grV?sIKyA+ftuIN~x1z&g<&pHviLaR|nV%RVc^mdi#AqM!E@dr=b1jg5Iwo^T_&<>7`%WkSzztU~&KC'
 'SGDPH?7<ZkEB7J|%}Kv1@8;WEeUi!o;vT2###rhqYez#il`~(RlLwGXfAIKjpZ+ed`X^|qPC!TpI?7fLg${vR>(yCLNm;)dM}W=Z`3n_A&;bT3@bnz30E=(LCm1?S&K{o#'
 '*}t)#T}=SsuFs*v>{Tp1`*#t9BC5RlUi#CY<`zA{W}uxlF<3-9*VTVQrQ3?)Ecs(q5t=<ub3vf#qY{;a<?~nV@CDune)QA^{x+98+F%`d6Yw>541d=lw5r*-$9D(D5cx7a'
 'dy7Yvaags?N04nV9De)d>^sELXxxF2S0GkR#?4<HTqmV5OREZiDj=07^}+u7hCLH}nz<Tel}djwyN)2Ic@{y0oL5^*uJ>2fepG&x&dH{Bgm??uOHFBa-X1dwt&|5*9V*qm'
 '%iR(xQ5~g>{ZKKwL$&X}J!TxNlrmpP885PZ{AVmOkW0)P(M~4*DkeRAfRV(nm|5_T9QbG>ICK<`8jd;fv!>qa5U^;usSV(-I%_fvP$L%4b855q3w2zVN2p&N=s5ObeXAfV'
 '^%`loExWVs?+=Mk8wedjwnE!*UiDvaeod*e)pQN#ud=|bzpv(~6q)5G46=$z{{SWWrquS(2|dF{KF<eBJlX$LAB#GXC!vy%ZKBe;MvG~wPCzvIB`Y(d^TB&-FS9j{g7vU}'
 'BJ!-7TO}|00Z@!-e~<@{J-^?Hv<}c3+CzRZa`(d}8FlDJSWn>+69XPmZ6b1$B?Ko}ns|wo`!bHz^$qeB2k3MK-cx7|WWcupT|zPet#|Aw{p(>qmvDg6$vnG!;tOyu#(j5>'
 '`3Z;)w8(pxSKV8DcIggPQr+=EgDk1HYunX?@{;q9ryFz~y;|3CaKYSHVL3@D>*=H`s340R9_RtAI8=~bDUp^S13u%)y0(ye1R31h)@>oXIlHaijRz3TZrr{bWhW)Cy5)JY'
 '{2bItxyctafcq5~U+@}h5%;D;3=sPvVthcvs3P0#3Gov!z!QK;nV@mQYp-kj*cJsTo&M6BWf+DMjt>~*{3I=8iSzKZN{c5`o*t#S_qviK6Xk5*obs!L@6XA){3NHk!5o+e'
 'f(E>rw~=q@2*#l&=DVcs^->2ON&PA9wi<$!{i|f;bdnPSBrDtvGRkz4D;z`C$&iCK12$32aNr=7W;CciF~b8Tw;#VxS<1d;6MRGLg&CCh>vF~sl#o#tawgrsWOqF!H!OL7'
 '2Y%Fx+`pwKTl~H^ANKb6>H$Ne*ZI2xE#cl?yj`z-+a)*@a*&HB{XU3_$DH~27tJd;35Ms(5`Aeu)|``GZX>I!i&@BeavX^~_uxo+4I-`0%^uVNM5o!{MtXwXQ|9*TR)zse'
 'F}u-T1pfd>Q^ld&jNnJAoG!=w3}mlX=A4uyNiVun8r!LGkRt8gh3KMVxI?ouDp1*vboUu(HeXf{;a@n_zvGKI@@HexT&;)lAF2%cXMF7ZB(2KuTW?lJ;9-5<^kZ(xSVx9u'
 '42CD7WBEB+8OM?Dhw*Jj)Iu3YZS|8cDb+q?76wS)FXN_}M3ynjB!K+&h?ALuCswimR#Z~4KJPh2vwEoR35RC$aU0}sFhf@{-Z0xg2u#ida)f9O#<bu45H5|aDv`tBeEAr5'
 'vFc8cVSo~G80|!ePwd2uLaEgoBr#N$S6MrJZz2S8;81GypfQP`a;QIkn<FWslZ%JAP3NvJ6VHH9k6k(s^H7R-#oSMkEqk)YM+@etO!zrZkTJ0iKEY^55?sz$qdx*`Y;=i~'
 '$=hhcWQ!P=Y32&}q3tqkrEhyzvHPQ{p*zG_f^07!Us-MX^OUB(qW46=Hq4sv4?$hPKYMsB{P*yRFd{vke!%+qvbh76CgPAmTEVF2*-mC`)VE{bI`DweY{$Mgev&Kbgv#jN'
 'AifPyZGQ>fD%{&o{`?@TQyTy|PnK67L7X1sl!yoYVPTJ7n)SzMXuZ%8VjNT^?yLI7l$kc_1Bh-jrb!$|2=nCCiy~0_m*%Q^aw*OO!G#;@%??<aIL|&p1EOe8w$%<J(}J>`'
 '1Bk|i#NA*@$51S_!x!N-B;zQgc3I~<x-hRg8J1Dq>!Zx#USHvX`%hGoxaY6(+a7EYb8pbly`-Hd7&<)xl*{CzP$t*hkqyM7TwwDLY1SsjcEHz2;(%{gv`_dQ4uQtM-Q3&P'
 '58-^V_JK<I3mCY#`zqK6;;<QqQ40N?n&AaG?55Seh0quAFP?xX<t%~bV<rSX5WxBp^B|s*SN(b)CF%nzai_wP5u=g1?(d8M#g-@R3a}6(%&hxYE$pdS$|X89&jDj#@2BsZ'
 'AA%ab0Lp%BEa405hA&_ckbTObS<8m}yswgBS>-{Gazra_O?)Jx6-#pCTIdNAy|G)hh`SZJBG=BZ9V!)v!V@pwWm(GhauR$xDw!<CxLuAz@ZNpK<W4(PqbLQIS4v&(hbtzW'
 'uin6#5X}Z2T{Lmd&U^}Oh}{o$JYF`8+duFW$!>Qc#PqM5)c68QsqsJ~{0#-=B4@n7P$EVnh9uJJb)A0-195OWVGswtLEZ_HQ=%tfK2cqbtJ%EDb4Ur8-%gqaaIV|Dq?t-{'
 'iD<h^#E@nr&U#xFmCisSi*duU>c%~YHyQQbjPgEaC<%yec%{ry`7Elx-K(uQ6!DZYYecy5q?rw_R}tH~TmZ&1MDn~Dax>k)S5Rs0KY1tyj$dGXV4}cKB1UQCci0(t-6ezC'
 'rX1)vSRmX!kms#o2E|JJQS5*M5K7g>YsK&03bd}Oj2YY6LvNT-=-$8Ef@c(3z%1`e++!&ZB-_-(ERL_9n^{^JbMLx&KOJfXqOsZ7j!0@j2dRhQW@XNgQVJSm_MNO723I%8'
 '6%<wp7{%h5vg*dyV*HE5C$8`~eJd`{Rh6fgK^I1T1Nvef&Wy-O*QT1j>P7|+oJ10N`({j!a#|@zu|_PfcADkxo)J=k1SM!3?ncB1sD9NGHS$-bqh<F$`VpSCipC*w`4cQo'
 'D>kZ5nFC*6+<Dbq&Uv6ee;PXg@ETR|llNCaStWiFu4c!oke0eLaeM-Dc^k{$eV<9+g8rruc@lJZyZBv#_Ohmg<+t_|VwGw%Jm&4i;DLV1`XEvw_CjxPC_`7SD(chik+v&L'
 '{)DM0U`6DW;8~a_9Lgk=j#4-2N(U!Xg5J&r98Rc=O2ftK)4IAkB~>4!w0?P{g=9$3N(1js6)4s)Q!vpXar8Jl(D?*QXI9V&6o+}$KeS8HX($d4OlFMua#}ffCUCI%;Aza@'
 's|<3o0JsWNX>SNdu1j8BJ#nKxN>Mr!_9+Zf*>7ES*B#u<SU#e6a}@^lv8WL`%5$jms=J(1g@4u><AaTx4jHz0z*h~W&R^xX0=X9cbtHMy@<1={Mh-lA#W=wNod)f(lfKp5'
 'BL~9Bf*XV3Cmb3xxqlZy)|&7{l&}IZN>vHNo%%>Hs6Gnyn8OG&!gH9lTQbTAQeLIUdQhsENR54@Be56Dx;e>HpM=U22&{Z;=2bhs@#9}0YtV+x;qs0R%%7q8xkmp6qTh%m'
 'O{Fg8oNyHnBsv=iQNDqq&>gm9Z$eaFLI>fjy2{UB)<v|AuMW-{)Ntga*`*KyJ7>`F!GAL4qa;+SgNJr+Ou9!?TNxrY$J(RMCj^+i)n!ax2_J=i<YTqdk9=>b^$CXN>P&5p'
 'sD*sZH?x^UZ-C<Q8Ry2&R8XK4d6Z*CRJxU~f*0gtbPk-4&Uly)UQjt4H71?pV)x?>^ejsg+mw_A?)9rthK#*twgq8OY9}&}<?LmLg$9SvTMs0JG}zg1w$V`Zv(XDua?^xk'
 'EQy!#!(|iXW>O5H_3YB8jdz5MGAyV3738^oB)vkojC$SVy-+)0aEv+#Ek6U)Z(bw)MTh}_wiDH*IBBO&<?J`AG$)~At=xma17BCY)@%H^WA+4uR3{Tg`9f!fPOIC4x)}!R'
 'c4#JGl#YLzoX+}%n0ri+eKFgXIk6Qpy%mS9DX^(HRF-g`oM8r<VI`mCPO6#;gq4_`Fk-!~x(81A{3KFC7H<<aIpbH9bNEN(tqpE7eq>^Z_u$uECagR&FRykZZwsR<wk=4X'
 'z^+Lsg0;w-SMH<V&YZ~echT?OKpI9@S0Bj9S>^b-{i~_JNlNb9Qu}^!&U0|v9SF&o?FvnvrqB5U6{&2wNd9+vgC~rPj`6R4)eRy0<@H%7>1h0^JQ&8vpzT+)4`%jb({eEs'
 'v>%=G=r2ikZm0sWN@B%u3~-SynNCxlb<ZiEfrM``lY|+1g?sl0#Sa{s>y2w0^IOb8B9ppPF<iG+SrT#-dIsxie86MPPx3m*VBgr}8^|C|Y`C+a^8`ev7~tj5+Q)Q~6}7$<'
 '1?i;RSi=^uINR@DO>U;33;|m?FXDE(QCwd_<Jzh8L&Q-IR=SpMbm88^s0>y*nQbl{69|^i=&d+Z7S78(#d+0T1`<*^p|e1aD3G<r(quuQSGE<0gRp-9-+$3$WZ!+dZzP_d'
 'q=n3~Ui&?b5U<<+0y8>Kz&;}SX)X84S(V3EMPr@0@$ng%v(j7-hBai!co|=yKW`a=hy4>bRiCwmzU_k=;fMF}N1q}$UhC}rJYDkyL~}*lY4?r1Hz7(T5O?bdL=D?%{i+*6'
 'j#qS{fe<$>@eYh3@HgSNI{fJ5Dj)xyTQUV1t+Kd>cgra`Ci`lqnCkvBlUss{5#*dn>+eP`zN|X8^lR?@fT7uoJ12C1)>U2Qaqjw-a>fIP4s%1k&|pT}o1%EX1ITTtYor+?'
 'FZ+Xbd7(0^-RuugpODc!BW0&_Ko0(AaI%!+@}9*Y5mAfU%%dIH@N_KPff<Kx2afJDnf%*c9*0aOdTTu!gYLy%`t#w!`rAz?4R;b#pQ7_`^ZSmO>gnHLCx`5uP-)aA6Y-+>'
 'E9i4=EX*v0mY-xKeX$`r^CZG0JAwPN$R4&kj(2AJCE#R;CzRG`9eNh#wboV8Rvx)d<XyR84;(seLGxDk*Gk;HISlK{0uCUg0}Ue;N4lI4nX08M*z)1XPq!CmP)aq&L8*6M'
 'eRzwaRkn72plh%vOKYCTt~BncENLM3@OD$}Ia`~H6<>c60p$D@IX_reW1qi^!&e9o+}A!tIh1-7hBsx6F<<b;Pq?@lhe}amx!UzDc`WIm3yRP*1%U!a4vbW@?LU*DoM34T'
 'dn^N{`cjWO?SJUr-%hpD6hp8SX?qbDKKmKJqaQv%nW)nA*v@@wUjU`Z#GM(QJ~6eRG+^Z#*t{AZ(DPU#ZaT;VmXV#sxZ|$3O~=Mri@-jx**vfNEPL$wF~W=F`eSLsrd6KE'
 'u6?25O(FD9!_mqy=GflI`*#gP1fDi$myBb!^JN>c4U-I|?%wX8?F)!utvko-o`5Ja$BDufL@&Lp;^JA?Cm0Gv$={X2hm|~8J#_77`(1&M4m679uz;oqN@NNUjXlit#QlLW'
 '$LiI9+WF~rCmc#SJ2APIR>i*ag?*z1m8!|8eAmfuz`cn#w;!CJk{l@t$Q&1Cb$xL}d63Z|MiTwOPuk)&?o=U>jyk{uV$8OzhSifMPdGG&8rnXAH_WiR@0Wb*6nsph5w<(S'
 '_G2b%BMK(Xd0qgTA$)pZBkoVqX$g#4M9b=B{*}Y#LD~FC?o}84>+QHaS1I%6g3-KyOj>#FagD(;gJbe06?)7zxfWDj`2tp~HwK9*`EJ9qqX4DKq;5(O0jJft-gf5)89id5'
 'b`Y>cPL~IL3Jy*6U}DPkviFCrp;;y548!RVHzRObAw$oSG{HfO9Mk0MXrguIsIr<cg&7qnUtn-x9;<da?^b=3=ETH3U5qkl|8`ef*L7Qw=ui^~d8)Wm?TLA+j&IU8(<<d|'
 'h7pf+V+?q65s)sGo0$z8<7snHI`n4W>$VPmMQFane{1#h@f^h=lfP<(<b7RT?!BIHT;3$}cUeg7)KxoC*rD5s^-&7ir25?;3wp2kq^04U6AY!CgTOQpUyQgzLv8;E8I218'
 '$zd?l6JAfSz-eCrWK{{hj^W0h5S-E@0q5+==>>G#GYB2eb=5zjH6iUdrJ}#5ui7)mT&5Q`kp~;{*ip9${SJx0&vh9|9IQ}idaT0oAWwc0X$no_G36zSrUknLubZ*x7CfdJ'
 '@R<JP$qAk(P&z{cLFhTkFMyYQ{hP}0W@WrX8xYy!T>ota6?DIVj8OaqV6TY%?)<_Dh)(EYqgO2K$=4O(e!E0iOD1tA2b`>ctDv+(cOyH~)E}(5Y*rjf+k#TFMqRbfKvwl<'
 '>|bqIP#VbFDw;<ilhFiv;!bAih0>F_H=O#OVzg~Y-ePbJ`7_QH0as9<#*dQEl2>v09U?!+=l}x=y(M*ZIr&<Bl2t;EBgn{?j0x^<yIX>FhylUHsrt=|NC{pX#T*ZYO0$?1'
 'q{2WYVu=Sduex5t!~Ir7O9NWJ8fD1v=H0A2)3H<&YkY5%TOR5v>nnSIy)Izscu+$gDRY+)eu-*0y*7up%X$krNXFxo3IWCtx0A)A2jAgz`r6?>P-(8kBu^1ItqzrYyBloI'
 'HZVmXW9!J`-<5{i;*l|aC)C^>tIKKaDh$@>%uYcn9_rK<?=MsHjKk<5cSqXfRr@xed>p5riuh3XE-a}Agpq1Pe4tUZ;y|6CaS)`+tMLhKMr~N?VDlsyafi+GW{RE@4xP<w'
 '<oJf!wrsp&&km+CV;h*wG3xDSpYMYdRhIa{lGxvjnXa+Vt9Ckb?Az4KqhW0#YTxH>?91P_wPK%dAf&TzcBno?IR@<nF7;UG4}sTj%?!iq6a@yC1KaXbFSCCc2kIdYcINd='
 'qwSg-z<HBK>x2L_$0=PgDYquZk;MBWteZd@nM8MzKMgpZyuD{kO6ilifUoB}oDn!_^~DXnS0~KCx?jbea_m+9D#>Zqef6qf7ybhH*AK>8w1K~x?*o-4T%0kY{093yZCC(-'
 'e}&W2M(5ZbgI<5;!aku=zQH&y$d@F|^p}P+L{Ch{`557@$#oSTD77id4&%PsIhAf^HmEp^4l<BHL>>=Dy~#=xCFF#IZ-IP0n=uUswnQO+GTP~L={x)^XB(VAe|$49Lw%Ai'
 '8sy~usJhBmZZ0^>9-`Sk(a@{7jeob3%4Vt4cdh&e=OB&HEQNranszrU8SrIg+|N$$0ZV5nMP}aEAv#>;5jZ6;?}6zYjOZZ#tj=3d5p)0novyQ-gK+yL+Fh1}LxePSoHk`K'
 '25#_akd<i&*Yd5j_+xJt=9|#*FS6fs9D+C2d*EAs1FRxvjfHBnKL{=suE|cHeKQRPQ;nhF{nHyiTj6<vrP-@~c&pBQlWLN&pUDAEKs0jq^6ZItP;=6H1LKvGCk{+v8r**u'
 'aGt0N$~e3lGQgLR@4qGGCmAJVFMBNeDqfzQ+j04da#5T=Ix{Mr|Kf+ad*5;$EK7xZefHj$dcD38^n2%?*N(xS3iBUGbaoGnoKfC+Pl1;gNn{jOC+19lD;er9vnjug!Z_j3'
 '*=exyjoI(C3zvkx#Fh+rc9Gdf=cjL-z8ye26uOmN6kjOg)w(?+bgOkrcCilZc9Y&((Wh3Qqck9?az#GPn=ZC>fTQ{_M<ZV{ed%Nx_B_B}aACXCGucSVhjr7zxC50@S-<>U'
 'E5AXfg=&e{mw}KL^x$;wljg1Pp>(ZkC&&XicP{e_?v=hJxKqek1nqkyqPG_TeF@nKU6^^5dmJ2XsLdhzZA+$6X>9EL?EsyoCG+?y3@qS#i|Ml9X2zklffChy+$p0$Vm-O7'
 'Gq9piH972%&Sv=&@OkjmvXX-XhsI2^OwPZrf|1ys&OP(B3VHkVFLM4LMb1iY{sW+xXXCbMLzskKdcUFt2oU`A)rR$nW!_WA@(W;hEa!;j*UqxuS5#lTfV@iqFMGXjIWj*4'
 'IfNVp7R7k*O55upAjFZt&}Pe1waxUMA)~^ZUf{YK!Fvv9OiJckq!;<-uZPiB_#5h)Po=-A%}SFVUOzV6U^MfbD@_Q6+d|xq{Au|x!}=&)+$WUtrw=*|=Ip)QlsC5}LNJ8v'
 'Y_OBTiO`lw$T}0``Nehhfs9X3jxWw%b@DC^ZHM?`dfy62i`~oV`_NK>NERk;y5i7n_JyNyH^|;AbNQjAcWv=UDfV`9Yoc||SQ8i$iIku8YC<~?YluD!bv=C1a4@qugId+o'
 'G4Dmd;4ZrcH-jE5x|si(O=G6Ln2gFPU*LTzZ2JQC@)n&~igQMP{|sQ~@6-n@%>^}-0N&sgOdQuG+(MHP+JNR2X7M1+t8V!fFFyyAdNRmZ9jgySwL&2S)1CYcJyruct>Oc<'
 'SRutfQF+=UjufLACcP+Nn^7ng0|yc2&=#<+1FVfOhha&u1Xw?<;tRmqGbVA)n<3z928IyP8fQFV1UB<m{rm!|3ZPh0$9)4Gi>Wt@=YyPpXe{*3ZZDqfBkK4&Ep%)P>_<|D'
 'f#ukxY+y{*e-6U^-t3rx=<a6umk%;%f7ubX`A^pQ&CLcR>r&2V+JDNZj8f_(UoPVw9luDh@gJOij*`fq*{UljQsR$qF^BdSu-_Mk5s6LE;5@$SI;KMs2PjZ!5VG9xn^)as'
 'O=mp);m%;&A52Y#rFexxE-vicym}sL)bIE$5jkWRJifZ$e#{UROsY*>z8i=5{dza6;L!91RdRF@vhC&J?oS?;M|rKBt#~9AmUdpB$|^XVQqDn49_CfJ7-wexQWHCE7fbXe'
 'gcq5|gldO3MyR340p2!WgCC$5u)RB9TVd#|30m<+pH_pSHNx3g|5CJ5Lt{^v6pmN<u22C2P-<U>6(E~6G#1%dpOVg&aoVY=s1&Ki#M~{Xw$iH8G^cF7dKyBQ{@E=0Yz(o0'
 '(p)$IhIS^euJ3s-4{}PxPNZTKIrG;=oC_dQVh>`(Hgl32xGdzeE<|I&#O>eL{G^|c(hSP#@B@kDsfB!=^>cB5ZDp!BNVP-3iC%lLF&CBj+pz>;^A;1yln3Meyt;$aIqeys'
 'aU=Na2l&+|X|*JZge8^7emO_?35m{<BZ$}2vWkxg8bNm<g`H+{V7#|u3`sjCexm)i0MWTwJ5S?aKcf4p9rCxlPkD}V2cp~!QC95}M<jY(oHk7{ki$c1)m{?WBu~6X5yQcY'
 '^*ef0PL(<EfsxV2^`jtqVj3X^BT}HIRk&>Np+8HhE^GheBlC1-u%J?k9AvuuvU>RfC!S6U)CsJ7w?nb5s=9(tGJLO?RuE74Eip}}uL>*`lN#)_lcHysSN(FUn4e>HGEddC'
 'Rs@XxlD7Dk<1#;ql#p4h_sFZLHY~qikTVRUREs>eOoE&SV(_IUXEJCUa&XaXx6FB4-kWgtA#yaw1dN`C@>*Tn9`<^UQv&w=G;&!Dm!slm51;ZNr^q6guL?o&*Q#C3=XC~h'
 '6}YN`>^gz3)x~^XuLo(x9Q%Rqyqapr-QvLa3rA2xhTg2C0-1Uw9`}~&pK)B>!U`&Wn*-Wn=F1t!cWE=cZ>u5bX3kjyx*<DWGrIN#L}{5bW=S~^dVR3B9)?}fjm2z7Mx%+^'
 't}_~F)kZ_|IO^)+S%)taMh&>jvshNc7eSDT^Sa@EzXBXafXyQZNh7x*;xN>S@YF$~qM0pUjr}xJUTqiy``1GgBc%*Su#t>&Q!(H(D4l_B*b_T1;Ggj7nu0S~<k*g=dqa#n'
 '-JZWBC5kVmLbPb7E?X~DsC)q@`%Bo8qV!%M-N{dUoFT{|1@*Cxg|XZ!oYXv*GKjCx#A(+Z>-_<lg4ogalkjyFVC+BHKWhU6$MhLYEiXa$Lq_|B$!TRSOcN@r4}=w7I6x`#'
 'AYU0#ID(cyX62oc_Uai4G6~k@c6+#w8u$irlo4Oo9nl{+3fAXOL_b&2^FpPwCc9L-S+TNepU8@xk!XDGJi`Hn=mNF0+-rIP&OksVY8c$dsHH4g<rGreSUys3FmjxXgbc#{'
 'PT_}ahLDBv(Pb4chm6x7k$?RO<mM0f1C~y?8+xkt75IyBNI&0psO>8o%AAI3;(8;=mZ%Bo)kGM$GJ@Qa7kC%Y5bTA=1XB$SFHlB_U*L{7udWZGQ7Kf>9W~M%&$1f72x8BP'
 'Su?t`f&v;rUY!4WJqPuG<*k?lakO^$^$UnoDn{Z0lKsAZ$5~TAZy7fiYTJ_Ae>P6^M!f_TbrMIRE%CMLuV>P4*BT2<E=@E>KSjy$dahBw0LnVvzuT;^-sqIK_#M>1diM#T'
 '?!Vh)94aZ>Jwf6Hk9A+wmvR5v7IFrn@mTH9W3?q!vtG_t@3LC4=wbx<er>j;%0dVEy;8S@>=f)Ep3^XNPvAHt&gna3{yr>n_yT*;ML!U`ub$hb`_7aP=qTn-=Q+%k|D9oJ'
 'La}zIpfTd_0IA2(wnA+Xm9d8%lo&m>EmQldXi0{lMI2;rN;L}UCUb&8=}+yGlDcG|uhlO~%YVN>9zcvrnQ5Px)!&bKE|Iw!#>4USQ(G$M_G5;n*^hbNj<g@$ew#Lz4J_qq'
 'mVN-)&8oU96yplL*%9IZKdL(BZScY3ZrLk|&&17VM+H&&0wmY(xN;+*@h^ZwG#DMZkVYcL*{uA5N#nY>mcXQgAFk&)0vS$&8ywLwb?xVbcZOt@(vR|0*`-6mTu$I+vJME@'
 'rf`;->c@Ry(nXcsexvg;{s+1iUO1di<SCmq5V^l(TS%EfXEx`HHl+^%z)dx}zEB0_3;3bTeXnP3iOjuT<}AA~`-~WKX(cU~SAn$mHyBA3>umXIkncyX<)!KCj5CBGTBc;`'
 '38zS)UZ7aDPyL=LlA`5)&$!vk7kG938-H+LOuWDWO5^HykplI#>h#1e*ozmbw8rZ=P|H_+zw^JY>gg0G6g}hcI>`8*2W%kcnO_&)fCsdOz!=_+RG4)J4oEsJK`ZZ5-x@5T'
 '!F99gFV<7W>APM=#cKHv)EvdKmAj~bUMQ40-q53W0Gmg2ykQg`LLP+9eYw0`IshhaPCIySOz7R3zI6b68I-1a0Wxw_G?TVJBlCnq6%}QXS+=b5`oUQ*9H@j$eLjw|>U*wv'
 '+f`?_1CuBKrn!7LU3GYX(%g(PM3!alFNbK(vvj>4<yDSKX2X{bpo5BDJ+<QJqOk*q=G6IM@b6+?%L@9!@r|5=jJZ3AcM*+6SBRsXnM41gj)?SgcaADMBT;Gz$yLl;hEUlt'
 'KT(Ym5xC8=%wF|b$$eYC>*PQS3-g^mTtg-ZmK-}v2-A@|ZrL?PT7$z~+N7q1DIEA3!r^%S+Rxo-Pcr_2{0T1EPu+GN*~o^yL82J!WS}cU(n=pk_6EBgm-pvgG7hDgK`RG}'
 'xb=}&<z2YF(+m;67wNQ!gLHV_+EjJG&{-8Sa^8hY$HGMl!$k<>nXHe*NOnGjalq0Tc9{pitg_^e-<<J3*zYyxe`sCqPtdDbMP3h5`QxFC1(gt|Op(1R9s*?W?8z9n8NrD2'
 '@t_O~bZ-DgE)5CABeQOJqkwZ2`F8i5&o~~iK&3W-Beem#oCS5u)ibmV#6ziDx#8{L?7zy;y*OtL*n_2T?ggx}{VnVSB9c`PsZ7|I<&$~ONJwQBVQ65Kyl0*rq?{-@3^;FX'
 'KyPhe&~G1FUs9Uc7Qk)3)^<3B07#QzxzJ8n9l;;9gzu|xO~K?CEd}stx{SB;(pPtfhdoj&D^EjsEKC4*kOD8@Hn+fsW7f&s0gLbK@<aVkC4&<CK`5QsPxRXyNOZmodbz4D'
 'l#X$`bW-vevrf_kea`>a>Sa6^ZNSa4QwTQ1qpTPwf?fwIjjfD(rH<r5=2i8m))@tl?flF!Xp&7#EP=}99%P20vMRA<d2JE%`uVIc999WA4k5=L<LQDj|NJPeW{e_5`=o^@'
 'NQ|OBHk7A7_z_~gTRVkVTTu~rw@==EO^u6?#3DmLE`;C=gjx3_LsITUkn1TDYr2zaaBgmG&p?!zM-i;J$2_jd!U-@0vgBOAFrQk9C7h+(mPm5DVP1_-lu+bHd8N?pQ0TFD'
 'Wo{YfeTd#U*u2@z9W*o>--;!luM~P537yO{y>PKjdW$ko+fVMS&8wXbmitSgD-ea_T3`v$p+qd%J=>cL4-!Bcu5Bqs&)!Y?CRJ8Cq3>+T)`=GJnM-~}LOQ8?vHCf!a&NXt'
 'yFH@;Pm%0)$uyoPcW1_QE{Y%G2NbJU%Bt_G`mNWU@+A8O<}kv%p46+LV4Z4T174p;`4TaSA;YxlE;}XZ&mw|#glV)blX0`DtAfhtUqCkDK$qU(9Tf=`ht7fzZVhSGD_E=v'
 'F{Hj==eIMTaIPP_K(acCBZ$>PWfj*4PAU#iSrQzoc^)8tRpEr2S4AB6p<uz$Y>88Ou2I^;TmmghJxCdqs^_rR^4A|v;m~^gUF$O{P`^PC&;7I-_0HCe)PrWEN%?M+eJyCI'
 'lB7zI)C13!0sVIDhk5gkQSEDC6pL&#ez?vgvXAVd-EPil`jeZ!78O{Us@o#t@o&IASiMyq32N*T2}D|@UgcSvkMnG?{IQ0o^KqYAJA45W3yJCrD4oV8+VYjBo*$*E_fvt6'
 'dcNJ?(-GgImUFjdyx9Z1R0W68X$OFqx~i+|9fABLREoLDA-9;hH@tj}asVsKZp(LrC~36M1Q81m)@cV0uKD{^AOrdf=fpMtV?5JP(j=Eorb#t$$w07eKK9;XYOnR)1>o5O'
 'AG#%4U8!RUM2Psi#ZLE|k&GkgAcGOf^Ycuf&LML<?^ng4*^pQ&-NhIjT*f-n$^I&F_I8?UO?UxG=Fp!hQ&%6LB`q*WjTi=qNgAAV&H~xExl<+tp9+JGoy~84PMjLPfERWq'
 '>_ZTo6sWi}n3YikHDHMatjv14<g0N0b>Rf28x+RtIKXlse6U5)XcUHAh-~wxeMMP*%NrI<sE6-m_|W46YYF6tEJ55Xj7Jx&o1>5`n4A)R7@ed2VVZZBOq2&HPtGQs)C!y~'
 '`%%+`<VvEBWO+L43D})cDIH2^hbm*=C?dbEt<j1cty)RQi~tiXpFjA5p+yWLuUT1*4~RHD2i1TF$0mWq_IYEMb$Jfy0ekV7Ot({dL?=L_6Hq3I^)nE4^|C&Nf{VCa8H|Yx'
 '*3YFQPM|LB29!x`{lq;S!^T!m`>8OzQsmOvURRgXP1Gl$60$Imytsc>^0;ra9MG8tHv$YW^jG1FT;`OFK}8o-LCLlwW$3Q+YQaaco{7>hmlxf^FNw8^$=w1URU;_9ah@m`'
 '3<hw1Q+@-VgwR@Dy(nP1BeZrxqBDMAheBUIB>ROzDQ1y}>{!LmsO#NzvK5Ge4m*E$ct++C;fr|#PB?U$kr`dcj4&?_Z=Z00PR{%mR&6nh;q5!i>rX)Rpo6R&-O{Qh%E77$'
 'iN|;QEt=4oJh$+4OgKPkID(K#1$i9a-9@oI>0Ds$XHe_vGfqgZPEbJJC%2hy-UE(tZ0q|NPC!VZ&M0@d)YbKEIrUMjI#h7K3<km2?W-L=*Ct|PFq8bHx@j9<g|I({Y)-I1'
 '_X<unBYP4BX?-nhMFL8}gOPV73iBb_s+bU>^#)w3Y=BoR{-+!J0ZO|?#WKxfRq*+$;?Q3N8zOmNlNfdFEs6uE*L`q8xj<c24?><)9B`^4w)|Z$&>7`bzRZE77^~q>Sg^^('
 'dtSk-xQz~wV(1~S+V#N_Z6+aqHH>Ube@4+FRS9|<gCMl$=2dZz-j*(_D3oH(C8m;YBVE$FYzHR>G7y62h?|DbbqJh5=`<UiQ0rJ%6{X<B7y^)Uk!+;7LW25SQ2qwIFroTb'
 'MO;47z5>x1@;p#eGUOygk8igp)RrpBV*VJQ*&@D<WWE#U0(WM+d@Xe97Osgxw*swb6*?>r`eYPZnFk?_Q&;&3z(JG7fzCwE25?i%A;dB*8y5nS!-#fX#X(g?1!Y*G7_85$'
 '{sD38dNT4?z3kDF&{1-8*;mEEO32bUl+)@HS05_wpUzw-HXkn<zdVL<eNgbgp|f^KqrHgq&=J#2oEqsyLnGcIw(F}$>Vs}W9G?iTeTA6@nFL!1Qm$<D=BI8RA2>(kotqcA'
 't`_3}h?2u?cP8fW$i3!3qS=hL&~jS|HO}RMzX6RZ)}H)bme0d|70vqnXxsATp>hlY$9WYOmysL~(gme#d)haM)4__%eU6NRbb!TSeNYC{DOi#jO!Q=(182z6yufv%`%d^t'
 '!oM?jSogqA%s?~&;}5ZZ3LV@7j$X#0y1VQn=0LL3t}wYO(|a@V)dPseOpihPy$|G*)9#}dBQk`*&Y8rxHrJu>{R^<1ad(CD-!4z89yoNEQ{-K`#kfH)|D7h#wS_h#)rKgW'
 'Kv(94FFzMuJJS6Mu@luoo?>_J{pCY8L$yG7i=B2b+i}GU{ecaZxU|2=FrQvMP$}QQ$z>UN)h{1me*&U8YFu04@(H>l_66soYs-Vipx+DryLNm1`RLkmiqV#AI22krhqvyy'
 'g_atF-cl3D7lK{P`XNU%hOen^m!<6Y@Gy&r!8{UD$T0jJh|N<Wg|9H@u&;F^DckkF-T_2oGkiN1z6I?b7?fV01WGxx?0ERLcWk9@04rS&-*%6wFyHOneMs&KaF79qOQOAd'
 'Oh$siO2k1RdD~Z0Z7^OAg$%(1mH+rPVq`|IgJxhM9P=Z+c<ss~BOmZNd^e0L*^lhMiJ;vZ3CKF{?s$|E^sXu-smi-g(z^(xY=88jIMvR&>07}8O5>jH-yN91{-EL|CJ=+#'
 '1SVAeOU$dtjOrs*ZfYL3>4l@63;*ZENEbZ)Sg><kUMEnRW1d@z(#{LGaY$#LXQDnyS1`8^@f7pAyhoS)_`1%2X1n<kr)r$Yd`uJnnFXezVx`suxmYBxUhGo)7vRNeY(t!z'
 'czFV4R9e$8OEfQ#C+;ks(phn!QfnIC`4)4$WUad+`H^85othxZv4(vWXJ+Q9kI(~%&Y(0?>08j<S^&YIoO-Lz_CyOhXfHb~GaA-a(t6?cy&?P?r;wE-hOo~a+Z#9gHYBgw'
 'E}`4ab$tgAjok>tj*5MCITdh)!Ada?>iY63K49s>7lnMG<pbzo_WCRb{u(<wq@%IUc?cbfn4vpw>;7;<ev)zribF%-n9GJw_}W1^HUy5jYXdQduQs6hxf}Ty0H@Op9C#Z+'
 'C?jfliU~sb!>3_pKbz_d3UxxyW5Y!J{6#nQ9AvLPU2qWP341-#MHN({a(j)cMEA+jdW!gH@oQr~p(UMZ@q@%$bhpS!%hM3sbMy^z6wb!`A!i(@gdF5x{4p|Wz0r>Cm2U(Y'
 '2K^kdWp@+iI@&aovV8iZPXAZNyow6+FCeQ)GKQC2oZkEG?u!&Hd4^ec>O-a@^vS`EBKoGzQKH^<K1MQT`vmXo{0W+2DYq#Z@eT6u5uw2H&VYSJp#faSZ;${Ou9=uNYjR?6'
 'Gd!J%s-P4x&FdGiy`XGUb!5mfSPc7T#MyY&w=ZBt-?qNMa?4(x1WKi`VpvdA8sQfI`;lL87$xNJMk3Hcj&HmreamiuLjA#G7!l5CdiZo&`igSUu=l0DuU-u%xaCIi$-*y-'
 'egCTDoEPGBf90H?MnE~5cg_naRN{AU)b}lZH#x>sX;2Ov8iSHifh_aT8#eQXnf_wju0cmDS$s{!n|lLY2)}$6<u`~uli2tHhAo9naU5=&6{bgU^aMN9n<)Cei4xvivh8ht'
 'n2FZ2b<QJL8zvpGg30UPClem}VOltdm)8@rR3N0XkHEsXxxCsfuD}0O+HpWs%2^JF=T#v=cpgs8E|H;cY7#v}i?da26_rxxQEW*ptKrJPZ;DQA<Nkxu;SCl(u_lo6R}e?1'
 'Du}`TbGpz3X4tR?wEoDpO{5Dy1vh#Dl%wW(73}gs7ZnIAF?;cvT2|dNu1phr8)A-P(Cc&FgpkbN?xZGPH9TWy?B7=xy@E+8ax%hf5jduF;1jKX6^X{YU|VAFi5h}__RNo*'
 'AQgtuK@PI}b*$2Imk~=lpT^<BlGh*sbKhOLy~Uy803Bv09QXL*PQ8lzJ0G@79Yn|M?d*2kA?Z$jZ21LRiGODSf8#bT&p{nvj3TvXS+!TUibSs)HnW#L!C8NUl4n#pg&`v*'
 'Yd=}v7h1priO!r1I?g{?;`?_YGL|d{^qZez@D-B&x!AJ~0JyX+SZJ6{w|~2?I&Cq6>lY(j>B<Sl<+*d1kG>JFtILye^-0$eE-ZY={rN<7l~>bj)F+WvGo*fvWA%!oXS)wa'
 'q7W=bcH1?-Ga~#4p{trV@>he<XNNDk=V2p~{R6XI?_;{gc_eEm<L4(-MyKggP4Kab*C{V&w|8)H|JTnBi4xyH`V~Zh3i(*=a85S{@-(H`GMiZgd4i?;9W?V_S}n3Y8oBmr'
 '!S#<1#?H7h^`Tbx3&?bHl6glj5#iohHF5%?Gw|u+7v+t+ui8gI`%j-y1RaE63yvAIZ!9;%gF#zs;>GULy82xGK6Y5UWzELYl-bv8ENnltn0V#{M5Ec*PN;4{i}9d)U6+i5'
 'lyV+J?w0I-R4q8W**#42wh7%-wES!MRu~3^g^~K2-4Wb&2eo!H-N8l(v+0To%j@2tA%bq+AahqQ?x(Ieg37Sq?j53x*}mORu1YO_>!+zt0xjnE%t?#c_v(f3&a0e&7;^(?'
 'cgfhXsl?_VMs&mkDUa(XEBL>D1q7kH<5-2ogG4I|quY=OqYatrxZIUT7%>tDw|TWp7^A(xq@$`{u|n)JMb%Zl%4`|4sZ9AxXzX0g`3V)M_G&|!`1uA3#{TU{^1=J)kW)F@'
 '{HfG%CSLpo`m~$4^U^|(t80aQM606RtlyPR(u{dAXRnC3D`YB-i6g-N$~JE1kU8M~593_^`FN^=Ko1WBXpnWpO4CH|Y!f;G(I^SEt%xn?@D=yO{EdRj%n_Bqn+B2wJD9I('
 'xPA%yR}@AMIEWgut>Oi#8~z24&SG?qQc=;1vGDi9pIY*np}ruDj0(~hsg~cv&l!eS`H4Ft?ZB+`&mPeq|2>j;u<+_YlNo_-YLb7Mo2$f3Ms5EHMy4dME+^DJKr2Hkwk6wt'
 'L!DQ{@`{i8D3u-Azk24ZY%%*wa>x{B_5f~kXCsh*U6Fq`BL^yt8Oh&uvedaZDYc#TVTsUgZn;5bV<w4W#5WlH3I;E5JzswFRh-=%+C(bgqu+iF)85KgFVIw|H!slC(JzZ^'
 '<tLeSkZ-O@`~|WfLgvRHJ0;{GXKSr6uBojT_GWXya|;N$Xy*fP8J1DM0ZJ)yTgn-qIEW4;q%w({WCy6xJTfNhghYoLjX;85SMecPXC%7gCyxjeQTw-&-DTH#+m0%{#LVKp'
 '#P!{9B4S}1CP*{_yqtle!ayZr7Kqf2)lRd=n=&WA>eT(l)b+r>RcuzVs^QLdJxKoK)%8l^^y!}ct774y)3jpKA6alV6}xk4Q;D~rk2>_gvWn+b|9}(aS_X_E<S08)uJrD$'
 'j^4Yr(_sqmax$rJ5u<ryUTP5M76TX;(eP$G>R;>Uqk;!2Qkjty$3ao%eHxJVc0<1Yac2H<lR<&<H!!FHa_vdlPnblo;K{^PX3HMkbS%Z`%Ya;&g(<4gKHUC-?9kOZRUfy@'
 '{<ODpfKqCZG->yEfvaG6;><U%AHw<4j{}wND8;=4PJDy-lxKS&(V<R}L0NIqDrpI9e~-@|Fm$StBZDGjkQfect{yvps6mSs(hmxQDrmP^$hd7#7c7%8bp-juq1tfm$o9qW'
 'F+wSV=*vtTJ02FyelzTD+oTZKL_gIRbi$-jb3aU5<<-kF3bL^lsv36F2b9;Ru6&Bepp3sy`F?+Rf5)Nob$3t@Mvx&czgpzyunusZoh|7&qI8CyhP`i!!4yn%NP+MMWA3{X'
 'y(KM29vPWO*v#saREC9j(;<0YER^rvttDIxm=16dy7lTRjjPBuS1};F;L-RHdHa9nYswB-n&KKx(~L!9m{;KuPlfjswl49u6AA;#sA+lAUVRSgGzF|2Tj5f=2NynhM25|n'
 '0H-1K)=xX-8I@M#4O9Hv8|-|X*TEL+tfzF|Gh4uep=x5QVo?49!X7J<40>LrZq#<{D1NQx2M~=7{|)p4@Aga3B*$Drp)I_KU{Ew@%LgFQd<Y{y*r;?plv(kj+`}Umq0k*!'
 'FG1ccHA)wuuy<TOQ$TSbAsuQbr#O#QT3_~-ALW$V0RuNB6AG&bC(aa2WFS@xIto44`)WgBFU>MYh0MmMPw+I|z7)D~QRxK`1wuzGG{0uObC^*C-HC$(HQC8H@w!aK2glAr'
 'h|$^;ch=8kDjAi%;{_Tf4k3^m5~FjB-hyMg;iCNA3xv_}nTH9`@-DptheplgUWG%l(7Rgm)#YA+IJUfRvXEmt*S7`St;YH&W^$P3F?n_n(9UZ~MrCv_kVfVjd7n~md`H{i'
 'VO<Dp_fI0E>1*?jN>@9XU%<*QfL{#qosv0dGq<rFhulK8(q-aaEeBF|(ZoVSL9&u`vir^PX-_wOPj1#dJTPgl`Eht*j(c{@T<+3noUwkpxNZB@g*z06)K=yKKR%Mk{SI*y'
 'CgWzr=90numQKzChwku6#amLF^^W<WOT>YMRH#w5e}w@WulmDM7A0cRZ1fg!UQtkQ3!BZ?*_u}G*!~<0+MlHCFwkH&+UZfhvQB-Ed2j>Dt9)g`BZAp9gf?e38gSFD;Q>mq'
 'JCi1Z_yT2h`82l^4jpE3CQM#+mjg=f@J$L1jp5Fqq+3H;<+ngVKlS5q^J5)FzkPb}^A@qfQc69DbQxvUJrGu@Kon*?u_A+54V+d{Ixx?22cpdl56(p&Z%DaSR7Up(a<y4m'
 'wacB9@*vIKCo?1)q1vqQC*xIfxFd+{H>pEN{K0nCWvQriS|bkT-d7R6Xhr!oEL2cfrO1O=u>uDP*kxv!JZ!g34g?JG7Cq0fbTUU2TT$yO{Vek@KqD%fPXXFq%@(wiipt&x'
 '^3)$Dn%}8Sso_X%lB0kk{ASusKbi=P;GulJiu$G-uM0ZRy-23ql=MHXsuOXODxZ;PRDMW|lvZ8O%-lRetK!hyE7DiJNSl#Y0cIv<=yW{8U^QZKi{{n%5Mte~+$p8b-99a1'
 '@%(&?@iTge#`5;*a5}ADQ7HmLg`65?na9h%Nc#s!5P~(^rD$__@rGYO(3K1&-ei1p&l>oEy>R)PRmVR{cYij4i9;~)a{}VYsw;OX;AhdT^aAef2HU=X>*eLz9u`jl0rCgC'
 '8x#oMhwTg%2;y5pzkPuyH7wjm41a=QRFDVt<!O~4>XxiXl&FJPLz!36Xm1x&%8fx+MOjF}WrI%Nw59%9+FVd5?RhgX^%F2WP+D7_M3ayc|4=)sY~7)$L!2>L;nI1m0*|Yj'
 '=r2TTJa6ZX2eX}?Zpwa~KwX-=3bC6E%B$`z<{?q>7>^8`*eA#voGb@9!MZ&5^LM=vA<C<<S9sG-3)-%sX8Rwc=~&TB`Yk6=y5B*nGV0oJDh%^NJ<B;-6t@GP@~4mVY0$qb'
 'lsmyPx>vx_&iyKH`{T|%$x5|z!3zPdx*FE!hw8Hw)qMH8EL?=BL%S}xxDhskgZpf(5aJC&XU(nX0NAgQtMKv4#kHvk(V372tXRWyPCy0al=$OF{79M`2EBd&{0-udYVo5R'
 '!uOYCTAQ(tuRqbmIsc%7QsK`$(?-BAu~6S`Ww3Qbt`JLhap(9mm!U4Z-KnDmw(J;#o#c0_s7U_;K^D2Yyp+y+>J^7pOT18BNrgGvEN;-MD1s93o`>t7|9REx!-&5PSvld*'
 '9Qa2#!U~vy9=Ug(kdO{Fy*0bFsI6wK$7v`=uyJ;*#|f756{L<yI{2%vkI?O+;e7cFfp6HJy8g)n;^+mu#H)1<bCp)a^l1a$f#2!zvrxWOSN-)bpiYJfX>8*iFTk2=E_yMB'
 'OPe6A@*|$_fkdbGO|^Q98hat5`>lFHp*n^#?=Fi+KVYvG_udS8h2fQm=`C*1BBowC_wGRgCm@>JKa~c_zBbdouP;E;-{yxiI2-nVyrG+3;ar$9eg!KGY_6+uT)6;$(Vv=c'
 'x1Tv$`x7eV8#sA$(q15{xR#=E7=r;ib;<qeq?58iK7UneFF(-heVL!BZz~W`Vva&FTE)zdSeK}&MWchN+Q^9Ek;@ykLokMjNeKDVi^ckayT>Aeb%cQ-Zl=Y}^l4LvS$qiJ'
 '4%IqbjuF^4Wr-8*u_EFdq;J(4p67v)4F62r(vON#G#*_cPV#p|MGsiIv#A-L)_H~SjA3bJyp6=3lOG@)MtPMMcZ8lErMZpTlr9i;#g>v5b>enHJq{!q^~ktb5cRRDY|Wmz'
 'e;$ugirUL&+_nv9LJTpGu_jWQSMQK#dY=wRI+JcoT4scvR@XjN9H@hwoM9Gq8|bpXY`ZU;5Rf+S$^ryLoU3a+q0;I3cBk)Gq32b8vrP*xoCgw}q9<o8%BsS>8MgOFJ~NJ>'
 'Qx!C_cDudTftTNrGYX|{H(p;OW9)Ki-R{L^7^Fn(<OrfIxr~dE6Pwk8?aFCWH@ty3e^wr-NTt{KPSDelxnDif`-DTc7tzbEtZm!q7wU;udaoZO+09nWQ(j?AEO?Cc;9$0U'
 '{cKst0ZFMOLEuHKs~5zv@W&?9L?_xKRX7onI8bR+#ubhnkbVKz)IIYZW+xm<%uzOn*?x!hSL6&sWe#15uSrtQTwu!P$r%fH0MUsV{7B3VJ8{2zl7o%bBYEc{IGF2zub@a('
 '1(J!UY4`%}DyvL3#7G!|i^c`yX`eQuLY>-(BN?giD;s!1(RDK!>EJn*%}H8?^{pN!BsxteBQPS?Re0dKlQAnT3c<*36ZA%sG@H{ISJQ)d#KNbHIDyuP@9ei?k&PeR*UpT{'
 '<~2NH+om&B!xfS$7Vh_Q$smY-UxhCkvwyXQ2MnFSQ?MIFfg@es>C+4~pC*ubYn>etmufZMw^@Uc$kB`xrq8Z%yD{`_&R~f6eK1msIDSDuIdO&XE&2RFOL&iQNx}TQx_DZ5'
 'eU2)8&%;m^m#U!UcOKS9st&f{zU|1I#}{1iY1+i;CqzHVQc4mDvZN(2=s@6|e+4dcCL9A`Ad6`2e9nYhKHAZ507y<ajJSkv))^pWx`A7|sTQ%72>5S~fgeCfrzP+63R}=o'
 'Q`Lzk-5?U3p!aV6#Nl?t;!x-;^tm0YR~Mkfe{8m+vV2CvlMEXtSjvsca?J#{W%7&q_D?{RdK2E<rxtX08^$Lz+kmm?oi!LNd4EH*4Y!jn$FC4a$7oZFzRFq24^p0^4JOrQ'
 'y_<OYBs#0|Rz{WjyhrjbGROP}mZA&6hY&3T%%_oAU#frk0^-oaGjsNKhC=iqCQktEWa-3r=>6NT>J$A_)+GbcSr;6Xk>}ORGP1yEB5nT*bS|)S(=povmC?R|m|M=Pw00E2'
 'Nm4pJ2_t{230JT4x76hwJNvy@(-a<Qv8^zi5;4gqPF@ZA7}g{~OnsylyOr;7dC)|7MA}w)hueAj#b8R<!!rCp#4wb(FDl4j<SRsI(~Lr@4Lx~GBPL@aprF}g+W|^rxWn$b'
 '2U$zH48jYvq`B1$MeYTDJ37V&m`7R&4kVi6CAQQ?Z>aNXxJ;V_aGV?}^1))5R`mQ>m7?!N;9}Y*ru1Ov+Sc-BIm;$EG{BK+=gA#*F~dXUo3Ox{eXQyUg(rGXV;id<L2k0I'
 '9pR(aH~Bp<9!n9A<`VIt5iMkV!kx+}NGW5IkINC2`knPlT4L-Tq!zGyE2Ui*y~;pPs}@e?%ZPh0fUbrbr7?i@s=)r1H*i|QjM%0snm5S#6k2~h%a8I(u_FvYcMZrwKQ;sM'
 'Iz2~_16Q$B@vSAsI>FE#_K3nP59)B5FFaxpHPbutSN&*X<|?<<RL3VE%Dp%!lZ`@`NlW_n8j%))qiF+<+KSmIPXeTa(VSLudRH9EGbhjb89-KyGBU3Ih04=7_X1H@8VP3i'
 'c!dg&vI=@n>er%<ibG*d@^?|NCTZ1Q<^}QZRng(RkFz2#n2b{7axS^7`qcw;>yuCi8H^ZOORM3sG@Sl?@SAsHnArwndTf$k1=<QIrO3r8J9X9SugJgFNK_n(8j18>xltpn'
 'vSKn&)$#4q0Zv|cDkj}79Z)lrRriRRYsnbWSK}K6LJK)QFbLy>L#HMv(=e4)dPK~q<Y7u*wZe4kx{42UHC7->IqyX7BIx(o9&4uQoNy>H4^EnW0tGB5Un(e+8<0SBr^>3o'
 'bf@t3V=AZH)Y(p;bf@w(5y_o8W{uF|z*L2yvC|(+t}SGGATU)>1f`5gtZYm3Cd}J!K%#lwToDQ$6VEI0Y>J_RQc7HAyDY1yZ~FQ@U|*i4V!JHg4MWu739Ai-`2Rl8t1GC#'
 'hdeUcd|82<=APO_oFBmRcA=cV-hlqD0EHq)!k=QJ<^hKU+C<(EvKwv4Oyl;<h6JbIknr9e+M@PvT|GkQ6o8{(efo69&MSJxL@G0w$DND#xo1%Bj$>9BO2{lGani6F(~D5u'
 'U1CyTkWRIm*_s{1_G<W<1?P`RT&LdGDLqG{4fo`6I<gJVtLv+h%cFEw9EWi=6zD`+xnp29(MCov#@`@!{36VTEt5z43cfex{QeuyF>gpI_IG~P7PDB1d@8#;l@z4p{O3I4'
 'bJd+z>^m$_y#jz3!-t)n^om<C#qi_i)&j-E-)6()4K71c9i=$po)^*IUb0khkP>nb`SSAo7vRe_lhijr&Jr0Pe3&f%DVSIXe>0^hR9pcTHjN7!-P`*IDlAf2ngR1jS@rAF'
 'gH>2Msmm=7W%Y@ycLk!9a~OgSim9BNpBNGIXsb=@XUU5yDD?|SdE~nKfGdz6rSQ}3F)F9a<a(@vyPCuW-U4qn-CEupu{fzWCsd$)gFZ{NJ?7FFeTWEqF(N%^1v9jL&E4rZ'
 'V74=rXHmG~^r%ah>-cW}?q65!YMQJ1Bvi^-s^hGy4?$LyZrax%rI9*623ad*F#&s8U0&l`pQW{Y;#ePBM%7$XqimPY?FvK`J)$R=^9=24!jDesEReI;RllAq?}UU@Mg+o8'
 '>hX;;Z7u4c*A<dVdXLp>T}dv<=9?IVM51Hvs;92-Sf@4lO=$2YW_caz35e#1$2}P+9r1Z}HSfjoAfr0q2a}U+ndWm<fIolJtT;#snPs0OPxYncos1_S8lMD9^N*E4ho1Y|'
 'rrRuWVSYA1aVlne!bB?k+1`_&+4+ci;^;h(==LLcp^vJr(s+^rCdxuLIX&S7MJH^Of>*W-?6~S^OjDnY579f92Fe4@+Z0qquku&DP+DDA{rG}>)|DRvh9@Ap4JtP3o>mtZ'
 'w;fM9H#Ltj2cb?Pug2b})-R45J?5Y)l&IeYS<Z2N|ELIAQs*c{@K3Ainx&cZ2BRv3pHJ=}6yDcWuPvdOai0z#8oNhrCs+eI1*7pQ#$&eYjqw~kfVXi_dH=;L0H|VXJ&^4y'
 'ki=Lprsnwim^^^o0@_Ml8h7%Ed=FY8kv~cL0XLAeyzz%D#TX#96%;FK3F~iEbw8js^U(m#DF^Ewup=3WPBoH;Bt_Hqc^2zALMskX%GnR)94`5*rc10&FqBdb@|A$vgv6T6'
 '_6dh(|Jj}G(}|h^M$vC>4Y2i$HbfIF(6pY|mLyx&6DyD4%c~&G_&yI-3wdk?d&__xCsXqkmRGqZw&e)vyaF$`fgggrlKBwu@=5StBSwK>=ClgB8`!T*N%cuisTMfMC)mOo'
 'wccoF9H<lXHRQOu)$&`&WaOi9AL4LzG^V<k0MQuhb`!{V7(g=0gVL3Li~gP0m2l<-;ULG?pI~@CJ?Yvmkb$DRlB<?}1<=j)-LL4bTWl{0J;=e4ylPjMmK;yg1-4yFAJIV0'
 'qjw*;03Sd|r5d@4R#c7kbKY%dn8ZlR$-`xFHzHm7fb@4Np0*y*&0on5Si0?)CnkuB!o9ML1tks#5S^mr-gFWa1!FI;eShTdz@hQIU5Q7U0p_^gjmi)5I_=J-%94}`dbBc4'
 '`LT(wKgcj0AU;6pMBXHP-(JAC2hqZ#hCpdi{IGZ@ude3%t`9;bVkfj)*3}2D2l+vuLkvc)63DCZ3B+KO)c}64f$+jPbF7kCJIDR>D9y>@Wt!$=70mKkxfO`l2|5q?u!5lR'
 'kv5SN5K;*m;SE*`dUg)C(lj49l$iZnnQe>N_s*6jnEjh4F~H3G^-yw^_SN$gMyUoj=E#$AAr6`<)S7ajQoaGnTXJ6od>}Bwg#lO-7~%HJYcbniUH`4E<@GtLBb-x2tUtQi'
 'mWd-^9x3=ckbp|exuj1LbHCtkK@yA$Z;4Jfa1iL@otfjBRNu2PXaEhPjWmDWWK>S4E9oLGZ%Cv^D&{f_uS7g>_qcfH7c5R}0sscK%+Y!0X5G~c3Y7TcTMoLFc<gn5+@H2N'
 'aA?eSC`U_Q=AoVSE~IUU3>`#HIDMxHW#Fws<oj3qBvhwWb&vL^lBjLV@hqbgx*rO?TgG*JN-~sLR~mB|Qbp!S(4ohPv`EnWg`k66gBu3r7iacbq(t~04EbQ9weuc$XiJEO'
 'uaFx>tswYq6={5U;$L77-o$7a5ID}Cqz5XUi2;&tn4y%D41L?}4GjB;w0~Ok^S<qYN~cDnK9OV9E@iZ>4>C%%K{JBfXM$L8Lk?Xg1;Lo->6Ij723$;rbAkanF%Q}jLR;C}'
 'uO~JEgF&}>ENm#ktjD!KRhP;Md-Y3{;m0SkA{m9M06La&Dt&7h%iT|@B0sl}pMYo#;K$U47PPzcYwrlW#xA#pBxWavF=N}GxE3)h5fAzi;%+&4JY^RDqPGF!c4dp6ZHP2q'
 'D;irClA)TUDwHS+9pDS9<C!kz*jD9?#11G&eWH>CmKjXgE)h%)z|Pw5Sc-5o$7C%26G&=9693wn1}AzS{dNP|Jd=9MpYm}JSVpHdUQX-mzXCMZw`TNdY91Nn?N!43&IPYb'
 '^5hv3R>r@d4a-mcW0ZwbYoah)$94uz^nG&cQ+#4`$G0P&=urEKcI}KxDRZ=PaBE+Eq&G3+a7xI2bcmwA%mt>5F|m#L$Ah2sv@#~6hwn!4JK@Cpd;0T}tQs;5AWu%s{X)i|'
 '1<c}Ml<4H~k_G-Q8M87e7*kMlAY(F*RBq%4X@s1_C|3~_KJV*s*3N~1lxwBKG-pBqh2P-sp7>?et*@>Bl%ss!<Vgrp+9$2@rE|M^DP-L76Kt5z0aaLvi*g1j#rD%`>=~20'
 '6}`X!ouZ@~3MV24^GKz4d6Ftho4*SzDoToX@*~Qfap*K7@!-k^1#EBB)dI}`tMi`9S6MV6IjU^6n!yvAVU@L*6_qGuzkU>Zg+WRwdoi1qSFcJ0Z|V1}Ku9NM9x5{{CYhsm'
 '9*djmDY&`%f`Un_*(@d-(&7HyK!zFn!OHYPLWN?hep#jUB}x^CZa3oCi4vy}p9kJkb)!NIc3w?T??WLAl%9F@y?b$88A?T>Q4}cY&c4tyalN}DuC<63h$Dd^3}Nna^U0+z'
 '%ls0(f1(!lOT_$kJN}gS`e{W6WK>0ViWUfI?!;>jl8&4iNbTuo+nv)*pcR%;sd@IN#8<Eqqx!uz99q9_j5&zS`><2O>3g()wt&#ii5uxQdO6ep?OdZp36cWCs}YMw&Q20$'
 '?3HGR2xKf-4(LtvINNqsP@(b#f>?o)R^!|ppf5IK2BK4JjIs^P)NJmuiVy;VNwa_;Mx3uAEN`GR;uuHAPF`JKwO)V08Y_-_wXv6_ZY4h8^Ek+e2o?Zhn$yLzTNT@uAX(-('
 'Dof5I5)_V#;qB|2qe96=<Vi;b)iuhA8s3QXl6mzT)A)0ZQUwAkjRBe6v&<KGAR$t4uu{$-pPBo7fvfQRcGXOC7|N8{;G&yW69DB2x=#=oNj&t@mLFrJ$b`)m-`*lRgR|}a'
 '1(a6&a#8!XO5bVBGy`k_WOUe@9+(C@`Y!@l6EuPqh~_B8Jx3wyI~l1{zGGM1U+?f#X*d~OaaGZQE?`%9s;_9gQ)1qR&5E1AN!Ja{A43FR;Rg}F!OLk3cWFr$<mJ(yVM$5L'
 'eHG1RWM%dyQBhnUm-(x4B;{0rvYY^-;xIbMV1)R0T@9B5BTT4&H%H6Q-8WCDE^WynRwP;I_7Mv5iUgIq4bsf6y6UddfaG64(S>_nK{vB2Z$J=E&0|#`VE%RZV#T4<_Djf!'
 'tEq@?hpeA@@#9(ZHDVOf!+Di@Gp-tw5-8o+*gMz`%2##IjFO-xS**d2movIHZ`Qa76gk+K=*Z~~mHFZY;z(tvTgZO%1*qaMddNXEhuKkswQ1NB4xRnW?w574nBD3r?I#?h'
 'z>M+)ow^zxFo39>ptFBfWa+D`%PYa^lbjN=)IWc$Ud>+E^ua?kHh+?d(-siBO;QqmE0oLB)$qW{j%|YD3UVAmj{1JxAA8YG($P49*wUo5pu;7N2>qo(_2Uo3&QbYYc(y*+'
 'X<uQdqVF5QeK6zswkJ%4q_BM-EpNIWeZ1uj;u~?@@C{PlMKXz0$S$t0_OJc=YAgNK+rqX$dZhRm-l`niUqF|_UOYK}yxE_CfI@4iB~|Rm++P;mI|G}}=g-)2vA(tN`MIQ#'
 'I_)uZmS5nT67b*kgj!mYAz~-Q2@k}bTG_TtC0K*TLvYSB7@AB4onUDkt}Ip<3LV!orqpNYqF8w@f)P>Y)wQGN^Mj14a>A7X1EM!h0yWLuX`d4iopy|zu4C1%9Zgf8<h6ok'
 'q0@0+ZCJ(@>ISPi8!M<FiWzNh6Sn!-0GkDY=%RPqkxBD*5LFy`u1rV}eXcw|0hcr0Rv=EPCkS$s_5eN9ZT#D8w<k{LWsF3#4Y4Owy4~3{vUlFVFD`kjFoIH1SSYfotNg&d'
 'Gd~HHki*Ds=T)U{&-mZRqm-HvMsg<e7TsT{89^;(k_i6lDz6@bcEX`C+g9d=%B$3y8_*8R3myn4`0ZZ^vl6#TYN(1Y4AIB+?zU}t5ibyfml0cuv*q4YpjDPCDIxoZgsevF'
 'g)+jt%3qMo%-FjFh|%rD!MS3ES+cj=JKHTomc$%~yDwfwNN!S$sNa22jxXfZcp1}4{HtUfFazMclbRn(di;{ac^AaRP1pU|K$e+!yQ#Zy%nSIfSf)Z!N*~^nl8yA6^1MZL'
 '8~Hh;2kd_Ue6xZBC^HPUtzv8u`-f~bfLf)S=Fb-KKF@^~50I!pKy~l3rCo3!W;Bn8*(+}?w(YiS5%U+7brZhnfdf=h&R25j7|%DywX88G7`j6oQ4TiktEfpDcy^A_K?D=+'
 'yqYvL$0IdCsZqPG`o}6>>m!x!2)d!{ud9u1Vsnl@WCEEmIrqC_^0&Zum!+h)#1+1S9OZnl720*0af%^%Q=UU2FU9Q!IY*{YQCXECcWs61+Z(u&SXkZqasr~cv%5VvBAq_2'
 '&4#QX?WrhKhff}H27)2?14{cltBg-Tj84h}(0Mhk9uNOD<|Z%FV1{n~^!^Gj*HQ+KbOQ!eb$RuPaB6-M=pn~1rQG&z0m4C579iN^-3X}Ojkz3IQtVWxqIO<?r$9U*QL45T'
 'M(fm7*3N93xhEVzbe0;T96ED*cIvYUry<fZ2b<mg)8T;lZVXd}<&-BR8u^TOLY?snlahs{Qt&i55oE~JySBGdPX#+Z8uZSFfOwxvn*kX0n+%L2J~4r=K1pdDG00_8PVfnr'
 'Kn}`MsU6hUked=1fM5VEB6(w8gPDG0=&h*qFK}i^X<mSg#F%}*MbAhy*Lz<|=&axs8hhDxo;tV#N4LqI5hL39ko|#5W8Qa+C<Rq~%f-D=?gtX3-2G7Q{w+|n?#|=2#M$|h'
 'FtF*q<wss-Rx%Ei@%AZpIna&_`D&`iMRCRtnvzlJGzH$fgxfdZ%W0zY7aY`rb#JVnElN5-DYb^>#!Kl1ZqKRZ2Wgci4D~#`Ib(gLA$I2Y0RudJIm*w~zQ7<wwa=w|B#f1D'
 'unuwqs)cN&2^s2~yh=&XD03$Ht2~r(d_>0SNnQz=<ff>7wKJuuy)%cn>2u7VuMtN-cSs$ebf)B&7ihW34U!8s8t1)nb~-ln^$Uo1iM)DU>0=>sBINKE*o1k*sJ>VH2LFSP'
 'e?#p#5N&?rcC6m91HO6zG3??CAOEo4+x|J5Mtos39JZUOy~T4xbqvP0OkV9P>>Q*+wg}u*=oHNe<mNj}K867gUx5Z?%6S+rtk2x~cF^TWZ$$d|j2PE*Cs!D%_)8vTOfq7+'
 'WQR%@JJ?L6LA(7Q7UQhIGbU8RN11|w+gq!U!##Cr#$!V1?@sT5NwZ6LglJ^(U&nVh7jipb69HiG(Q%ybp~_;5I{7z!qz(RtU3z^En!6m_5_TS`K7xvWHX1||&_L)6PWUb{'
 'z@>*n2Y#Q@BPl-OT(nVb(77EKG}LwoBl9>5TCd<e%z~Vo0B{SLdUIA+^sDXSQ8s^ZadAb|R-xMD9s0PrVTRdx>!+0)wVjzeKm-}S>!o5A)C6ZBMk{kC7a($5F|z9H&ku4+'
 'F%QE3d203LtZ8EAJTuE!iq7A#In~JkZU<t1zJO5FeCLF|3MA+-XC^`tFw-@o^*z1b2M|!{HVryY_EFnHE%TjKEZmYf%2LiF%V7lgDouocZ>e178}dZ?!AAK8PR`}#)%c`m'
 'l^>~<K9AG|K<Yid*?x!8iJ`IId1ktFmDmStU-fv?U$C+MjZCvwi35|yxe796Zp9DFCrno$tW%Us;7>AUp(nO+!JRFC$?<fk@jwMCtpSuT5uv?}EQ&{mwlHfba{E?_{R#Mr'
 '0i8C>$S!(t6s#x9)0O6-W&RmU+{!erald@?WnYZ}cl@br+&}l#^x+HQ&-LAs_|p~0jtR#$pUZf|mc1wEwu>j-pz*z`b4mU<sHh9c{k)!UyFrgOB0KU(b~<8|F$Lu>faOxr'
 'P@GQ~)uvGzOzjV!*9JG88#qAy_ja{evZ63_NAHk?Kv>M|e;cEMv_%_87vglk{zJ|2nu-|maj(VxspLKHZy=Re4AwW-r8WWE2i=*q$g8scaFj1(DuQLL^ProV=hRmJVrI5J'
 'n^_6IW9E1_hu=P%Q5>B=Kh1RCKARbn)#j;d^Q3VfJW8c<!mwGq(NAa~H_Gknj-dHcXfrcF7jt^)VFa)vj_Nz7h=|7Fs1~_|6!xQB0*o^6*gGgO;uk$O|E0LT)Jwz|n`cz0'
 'V$PY*u|QG%-J<l#FgHU2Bl@JL5YrLwz;bn>3dEK|JYe-e)1QW}XWV25;%tUSucw;N)%6UzsYo~)?VLU)m0Qf|gVbr+fbEF`%$TG4Zfv-hm$56SHZ81U-Zy6WP{MAv?O>3^'
 'A!HVy#9O2-o~8q;ippU1ijg`mp0rxP`GUiUSw7}#TFkc$QjdE|#X-)9SsT77lG`$T*>HgEK<~EKI|Z41*%U~YLQ0iOS1q1~{bo;$A66Vji|h0%MKC;!ia#D9xGj{5o`dK4'
 ';!i<EHlu@6i$CQC+<JDV-_F^E%xV~zSCN)CG!+=ph8UseCHItn52rWFb&Ot-g&#n96mgtUerUr+G%3XLp(abZCubcUdLeUPfS6nvj@BfvVs<Z+Pn1iN5qnT8J!O^b+<)Kq'
 'bW|ipFNx~3K~W?g3iMG#47f8&A>t2z!?zj`zcr=1R`22mu#nEilsq_x$)`jxyA%EiKInr71-^_GWnc?TC~YyVKp_8s{lPehuzyr<cPm=U)Xy7l!Jvmk?1dz{Bw;#<Tu_<x'
 'wlc=fbDNU`FZ5znB(~=kbmqOHrJY6<A0*yf9(Fdn2%P2*K^axF+C8Zy!*U01CSPcLr=B=$R>$t+iZGOKneSLBD2$IMaxVx#{zG5T*O696A!fj+@8=jama@kIqj3jEU5YZR'
 '6C(`!eF^I?CXVwCVicy#AJt}3_SHD-@9_VCAmXk?1WsJ#r<mLe(aM`zQQjYv@kq?L<y`p{2H7BszWJfjN!HYhg$11?X2k8~&kyW`8I;B5*poTQ>g6xcOcofHGGM{vl%(5<'
 'xSO8cY%2lNUcY2<f)D6wR#=uC9CmYX*rt~5+17p{tq>&;PWOCPSmcbipbeCl?b0o0bgVFJkU>{?F00X+BllBwhEG|+A?Tok=3zh6td4d?!M4Y<)ACkUmrFXA>!1-Zt6cwO'
 'HLZW^oG^@@%zF5Wg;wDO8?!Nd&5$}?oQ4v&`{_P%g1x11`;$D*zjGj%ojU6VYp;1kjaT65vn93)#$7i--+L7A=X{=b6ZGask(^#BIaY6^1|P4yRvflxk({c!wy(k+2T$8E'
 '3mhD~Ed=xsS?HNNu!*h}jJFSGIF#l;*%TKYhOr$YQQRa$VkK8tS7oTI?%74>w+N7!Db7Bc^}>c;j{Mosy#?80k?t+*j6yyIK<@|~y67kMo+>Q!Er>!eomZ2gL`alSID+5*'
 'Vw|fGR#0g4qNE)7kJUYZSX{iSD7+0YkkaOMuHtI?fC@v5h?6oN8&1LbA$=pTL-KLk7-jQx|4>aRqmz${YVSr6KC;K{jo_USczeu`NDg<Ujz}IU%$@TusKE5Js-@a+BR#LR'
 'P}DT@F=H@S9j~aYUxCIhoL6bOpl>s|iCZ{-G;}Q<4HDsW88VEXaONdjF<I>$m2yg5HIKH>y^}DSswo#(9h<*67FjO|c7tQr;Iv%o>duHGgM;fC9M_K(-hH>ap|Kl#VbMt*'
 'k;M9a6HkdDh=jnCU9}aKGv0wH$I-g_LW{x$iN#kssjR#jhXL323KDYOCE^f~e*(rYw{vS=%Yq*Epa=#Ww*jI43ex<;dftI0V#kB=RC-^)^2v@0BN~k!^#C%j#?eV{XKjj%'
 '2uKMWKjQVfEEHLdE_xV{SL13{n|dWOT0MQ70$a%GZU|D%?IVAa+`r%;)8?|5rb*cnspTiW&oCyJ{PsUc5@tB(PQOg)UNAW`e$7p&ZM|EO=T{LcPJ&Tkhs}U*$?V$Qd>_tF'
 '<*yf-K2TsQ+N|FI>Ev*AHLM;@x!|yPNj58YjULvf>gsj8(a6Vmr|&+NuNe8jK_(-EQ>wFh6+VbsM-vj?sb5`i7%`K6I?k)Kd^W6J3T(d7@Y%JP<FMuMfl>8(DKNdxNheMX'
 'Mey3a@j9o%lxnhbmX{~R&KN&PR+JXQ3lX_v_76Of_^I98HF~CYx_=_G(StQ`o%582o~r{o!;-G2NMAAN0S?BCQ(D}62wK*+1KmAGx_9S~rk<Gr-ZQh<%?zZ2#=BnSwsryz'
 'Ft8v-@t29V$y>0rTi4G@?-m>lJKjCw`LB{meB1C|L0SDKeq>2n{P9EaV_^=(x5OKLt9MS5elJ>~g^<~Wu(LFQgdY{MiHFsCvh-z4LfgpU7;qm=8A$Sxf_U6c^9=L5`iaW6'
 't0VTlfP0eKrz7xu9izz`rQ#Ma{ek%H+m3{a1B`o1rqv}K^IxJ_{yMvM0s$Ln(Q6M`Le~%L;eOlEWBH4-BeNA17(GSw6hvMP>(hhzLPp<ixc3{x4c)#Xgn<Mi-tjIM-Rr)#'
 '!p|CQ2Kd}YIUn$k)e+g=GsP2bBj_$>uRBRr5_T_P_C_~3J__1)!lm`xc+i}v*Pl2ny9u||pcZrZ7*SXlo*dt)G1+}696MG~0~j7icddDIntdQMCU0|;h6c5Te@klg{NbJp'
 'ovanI8ipUMIFyLMKn&%#5*B_d5fN^*p1b(HURL9yDH`8MTye{7+QrPheH&OSdOyf5QE?czZB@QFoQk~il9`j)%=>sG^e`(rQJ0FVW8w1`epDA{ZUCKyq&t5!^mfDh8ORme'
 '$&SOg1Lhd;{(yF@Dg+r<Dn0Ko0R{xx{-x=o!wJfAK08ET)h#@)z7WLb3z@tml-l0C$q&88kpv+9lZ5DjVj^(fBhQvDPf%u)t3|228ecNGttu6c@ibBb!cgYK7V~ypa99Gu'
 'Q0CKA%>J(Xx5?r&A%JrUq9+&Q6F2ZVG&j5fQQZXMU8M4K=Hx_d^Ai7&v1kGFux0jvdBF=1+rtP4KMm<t2C=t>eZ6O&IE<drc~9om^ul{GU&*?IBt_0AoMN8vW}|5a#5U|I'
 'CucbI3}H->?|(YM0Vn%TLOZF}L*L8!gmxP1es+4poscOK?5Q>g)3y3gvObnW@+qo#|A@w8Uy)HgmIFm(pB$tBPUh>eCn_-Cf#yMnL=&vv%72Md>P{1n8~~_*oxI!^9&e>v'
 'I@puyE_qaJBuU_WaJ-%Kfn!QCoys~v*}MgvUQ>>?a~zQq+!cRAkiFtLGn85}MVtMb4Y7ew?k)^?yd<7sm^?;Wh0YJehf))y;pEBXKiA=uvZ2qA#E3rVaiX+5acd;eD~@P{'
 '+!;H`GE+2Ev_|K-`~<-#@LNYuPI0LFTKF#!BPG)^UvB#)Qq|d2PE<Y2elY>bopJh*NgtLGOaHz9w;kVvw+(Rn2`PrzQ}cnzHaBy86iJ8ghrK~tgcW~&{H$zy1%*cZQBSnU'
 'LE0Ba%?BY~NDlFV=7SRz*&ImpUMFewbCqwPj&QPUvEvipfLKW3j*nue?MYhwdL<HINN4*80!#)18IsX|R632i`brL6jEowg2yS&e2E$#KR;wVMHjDEXfQ9WH#Shen#K<EU'
 'iuRS$qXCRVs+;ZUgs?rYeEfZ+2#`@3V*?cZyechK+rn_B>E9SYRtqnAmDVS8r9DTE5K|Yif6K_WCo^@=(0TQRLPfG4qJPPwwTSWMLv+3*+p$Xz`y5HF+1hB_H?ka?*0rAs'
 '&k6Ks$^1VbB}QWLX@A+%?{1K5S;*w!CoIds{{X4ZO8nABTWEAKj!LAq37|zBh9~C-5?dLCHv-lq{pzZ`u<_+91ta93^9E%b;~QSF!@#Eyr+19}C&(awguKScVBGJZ6DR$W'
 '$XjV-B~JS9fA>04^#'
))
OUT=Path(os.getenv('COOLDOWN_OUTPUT_DIR','/tmp/portfolio27_cooldown'))
BUNDLE=OUT/'PORTFOLIO27_POST_LOSS_COOLDOWN_RESULTS.zip'
RAW_PATH=os.getenv('RAW_SIGNALS_FILE','').strip()
STATE={'state':'idle','progress':0,'message':'Not started','orders_supported':False,'trading_enabled':False,
       'snapshot':SNAPSHOT,'exact_replay_requires_full_raw_signals':True}
LOCK=threading.Lock()


def status(state,progress,message):
 with LOCK: STATE.update(state=state,progress=progress,message=message)

def tparse(s):
 return dt.datetime.fromisoformat(str(s).replace('Z','+00:00')).astimezone(dt.timezone.utc)

def iso(s):return s.astimezone(dt.timezone.utc).isoformat().replace('+00:00','Z')

def fnum(x):return float(x)

def weight(t):return .0075 if t['sid']=='EUR_JPY_M15_SHORT' else .01

def truth(v):return str(v).strip().lower() in ('yes','true','1','y')

def load_base():
 b=json.loads(zlib.decompress(base64.b85decode(B85)))
 serial=json.dumps(b,sort_keys=True,separators=(',',':')).encode()
 if hashlib.sha256(serial).hexdigest()!=BASE_SHA256:raise RuntimeError('Frozen baseline SHA256 mismatch')
 if len(b)!=BASE_COUNT or len({t['sid'] for t in b})!=SID_COUNT:raise RuntimeError('Baseline 3029/27 count mismatch')
 return normalise(b,require_reason=False)

def normalise(rows,require_reason):
 out=[];must=('sid','pair','side','tf','signal','entry','exit','r','rr')
 for j,t in enumerate(rows):
  if any(k not in t or t[k] in (None,'') for k in must):
   raise RuntimeError(f'Missing raw field in record {j}: {must}')
  x={k:t[k] for k in must}
  x['r']=float(x['r']);x['rr']=float(x['rr'])
  x['side']=str(x['side']).upper();x['tf']=str(x['tf']).upper()
  for k in ('signal','entry','exit'):x[k]=iso(tparse(x[k]))
  if x['side'] not in ('BUY','SELL') or x['tf'] not in ('M15','H1'):
   raise RuntimeError(f'Unexpected side/timeframe at row {j}')
  if not tparse(x['signal'])<tparse(x['entry'])<tparse(x['exit']):
   raise RuntimeError(f'Noncausal timestamps at row {j}')
  reason=str(t.get('exit_reason','')).upper().strip()
  if require_reason:
   if reason not in ('STOP','TARGET'):
    raise RuntimeError(f'Raw row {j}: exit_reason must be STOP or TARGET')
   if (x['r']<0)!=(reason=='STOP'):
    raise RuntimeError(f'Raw row {j}: exit_reason inconsistent with R')
  else:reason='STOP' if x['r']<0 else 'TARGET'
  x['exit_reason']=reason
  out.append(x)
 return out

def key(t):return(t['sid'],t['pair'],t['side'],t['tf'],t['signal'],t['entry'],t['exit'],round(t['r'],9),round(t['rr'],8))

def label(t):return(t['sid'],t['signal'])

def fstats(rows):
 r=[x['r'] for x in rows];loss=-sum(x for x in r if x<0)
 return {'n':len(r),'wins':sum(x>0 for x in r),'losses':sum(x<0 for x in r),
         'win_pct':100*sum(x>0 for x in r)/len(r) if r else '',
         'total_r':sum(r),'mean_r':sum(r)/len(r) if r else '',
         'pf':sum(x for x in r if x>0)/loss if loss else '',
         'weighted_r':sum(t['r']*weight(t)/.01 for t in rows)}

def export_csv(name,rows):
 rows=list(rows)
 fields=list(dict.fromkeys(k for r in rows for k in r.keys())) or ['no_rows']
 target=OUT/(name+'.csv')
 with target.open('w',newline='',encoding='utf-8') as fd:
  w=csv.DictWriter(fd,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
 return target

def duration_bucket(gap):
 for hi,tag in ((1,'0-1h'),(2,'1-2h'),(4,'2-4h'),(8,'4-8h'),(24,'8-24h')):
  if gap<hi:return tag
 return '24h+'

def post_exit_diagnostics(base):
 by=defaultdict(list)
 for t in base:by[t['sid']].append(t)
 detail=[]
 for sid,trades in by.items():
  trades.sort(key=lambda t:(t['entry'],t['signal']))
  for prior,next_t in zip(trades,trades[1:]):
   hours=(tparse(next_t['entry'])-tparse(prior['exit'])).total_seconds()/3600
   if hours < -1e-8:raise RuntimeError(f'Own-strategy overlap {sid} {next_t["signal"]}')
   detail.append({'sid':sid,'pair':next_t['pair'],'tf':next_t['tf'],
       'previous_entry':prior['entry'],'previous_exit':prior['exit'],
       'previous_result':'STOP' if prior['r']<0 else 'TARGET',
       'previous_r':prior['r'],'next_signal':next_t['signal'],
       'next_entry':next_t['entry'],'next_exit':next_t['exit'],
       'next_result':next_t['exit_reason'],'next_r':next_t['r'],
       'gap_hours':hours,'gap_bucket':duration_bucket(hours),
       'era':'pre_2010' if next_t['entry']<'2010' else ('2010_2017' if next_t['entry']<'2018' else ('2018_2023' if next_t['entry']<'2024' else '2024_on'))})
 totals=[]
 for strat in ('ALL','M15','H1'):
  for prior in ('STOP','TARGET'):
   for bucket in ('0-1h','1-2h','2-4h','4-8h','8-24h','24h+'):
    seq=[x for x in detail if (strat=='ALL' or x['tf']==strat) and x['previous_result']==prior and x['gap_bucket']==bucket]
    st=fstats([{'r':x['next_r'],'sid':x['sid']} for x in seq])
    totals.append({'timeframe':strat,'previous_exit':prior,'next_gap':bucket,**st})
 by_sid=[]
 for sid in sorted(by):
  for gap in (1,2,4):
   for prior in ('STOP','TARGET'):
    seq=[x for x in detail if x['sid']==sid and x['previous_result']==prior and x['gap_hours']<gap]
    by_sid.append({'sid':sid,'hours_less_than':gap,'previous_exit':prior,**fstats([{'r':x['next_r'],'sid':sid} for x in seq])})
 return detail,totals,by_sid

def shadow(base,hours,after):
 # Accepted-only sensitivity. No suppressed signals recovered. NEVER call exact.
 kept=[];cut=[];previous={}
 for t in sorted(base,key=lambda x:(x['entry'],x['sid'],x['signal'])):
  prior=previous.get(t['sid'])
  if prior and (after=='ANY' or prior['r']<0):
   gap=(tparse(t['entry'])-tparse(prior['exit'])).total_seconds()/3600
   if gap<hours:
    cut.append({'policy':after,'cooldown_hours':hours,'sid':t['sid'],'tf':t['tf'],
       'signal':t['signal'],'entry':t['entry'],'exit':t['exit'],'r':t['r'],
       'gap_hours':gap,'previous_exit':prior['exit'],'previous_r':prior['r'],
       'warning':'ACCEPTED_ONLY_CENSORED_NOT_EXACT'})
    continue
  kept.append(t);previous[t['sid']]=t
 return kept,cut

def priority(t):
 return(t['entry'],0 if t['tf']=='H1' else 1,0 if t['side']=='BUY' else 1,t['sid'],t['signal'])

def exact_replay(raw,hours,after):
 # All raw signals must already include their counterfactual exit outcome;
 # blocked signals do not cause cooldown or consume p0. Per pair no hedge.
 held={};last_exit={};out=[];decisions=[]
 for t in sorted(raw,key=priority):
  for sid,old in list(held.items()):
   if old['exit']<=t['entry']:
    last_exit[sid]=old
    del held[sid]
  if t['sid'] in held:decision='OWN_STRATEGY_OPEN'
  elif any(old['pair']==t['pair'] and old['side']!=t['side'] for old in held.values()):
   decision='OPPOSITE_PAIR_OPEN'
  else:
   p=last_exit.get(t['sid'])
   gap=(tparse(t['entry'])-tparse(p['exit'])).total_seconds()/3600 if p else None
   if p and hours and gap<hours and (after=='ANY' or p['exit_reason']=='STOP'):
    decision='COOLDOWN'
   else:decision='ACCEPTED'
  if decision=='ACCEPTED':out.append(t);held[t['sid']]=t
  decisions.append({'policy':after,'hours':hours,'sid':t['sid'],'signal':t['signal'],
                    'entry':t['entry'],'decision':decision,'r_if_entered':t['r'],
                    'is_baseline_accepted':int(label(t) in BASE_KEYS)})
 return out,decisions

def equity(trades):
 events=[]
 for t in trades:
  events.append((t['entry'],1,t['sid'],t))
  events.append((t['exit'],0,t['sid'],t))
 events.sort(key=lambda x:(x[0],x[1],x[2]))  # exit before entry at same instant
 cash=100.;high=cash;dd=0.;floor_dd=0.;risk_by_key={};balance_rows=[]
 for time,etype,sid,t in events:
  k=label(t)
  if etype==0:
   if k not in risk_by_key:raise RuntimeError('Exit before entry')
   cash+=risk_by_key.pop(k)*t['r'];high=max(high,cash);dd=min(dd,100*(cash/high-1))
   balance_rows.append({'time':time,'balance':cash})
  else:
   if k in risk_by_key:raise RuntimeError('Duplicate trade identity')
   risk_by_key[k]=cash*weight(t)
  floor_dd=min(floor_dd,100*((cash-sum(risk_by_key.values()))/high-1))
 if risk_by_key:raise RuntimeError('Equity open risk not cleared')
 first=tparse(min(x['entry'] for x in trades));last=tparse(max(x['exit'] for x in trades))
 years=(last-first).total_seconds()/(86400*365.2425)
 return {'ending_balance_from_100':cash,'historical_cagr_pct':100*((cash/100)**(1/years)-1),
         'max_closed_dd_pct':dd,'max_open_risk_floor_dd_pct':floor_dd,'weighted_r':sum(x['r']*weight(x)/.01 for x in trades),
         'accepted_trades':len(trades),'strategy_ids':len({x['sid'] for x in trades})},balance_rows

def rolling(balances,months,anchor):
 # Month-end closed NAV, monthly observation schedule and event-time lookups.
 events=sorted(balances,key=lambda x:x['time']);first=tparse(min(x['time'] for x in events));end=tparse(SNAPSHOT)
 start=dt.datetime(first.year,first.month,1,tzinfo=dt.timezone.utc)
 month_ends=[];y=start.year;m=start.month;j=0;latest=100.
 while True:
  m+=1
  if m>12:y+=1;m=1
  boundary=dt.datetime(y,m,1,tzinfo=dt.timezone.utc)
  if boundary>end:break
  while j<len(events) and tparse(events[j]['time'])<boundary:
   latest=events[j]['balance'];j+=1
  month_ends.append((boundary,latest))
 result=[]
 for n in months:
  for i in range(n,len(month_ends)):
   st,en=month_ends[i-n],month_ends[i]
   result.append({'scenario':anchor,'months':n,'start':iso(st[0]),'end':iso(en[0]),
                  'return_pct':100*(en[1]/st[1]-1)})
 return result

def read_raw(path):
 p=Path(path)
 if not p.exists():raise RuntimeError(f'RAW_SIGNALS_FILE not found: {p}')
 if p.suffix.lower()=='.zip':
  with zipfile.ZipFile(p) as z:
   choices=[n for n in z.namelist() if n.lower().endswith('.csv')]
   if len(choices)!=1:raise RuntimeError('Raw ZIP must contain exactly one CSV, not an aggregate accepted-only ledger')
   rows=list(csv.DictReader(io.StringIO(z.read(choices[0]).decode('utf-8-sig'))))
 else:
  with p.open(encoding='utf-8-sig',newline='') as fd:rows=list(csv.DictReader(fd))
 raw=normalise(rows,True)
 if len(raw)<=BASE_COUNT:raise RuntimeError('Raw stream must include rejected/blocked signals; accepted ledger is insufficient')
 if set(x['sid'] for x in raw)!=set(x['sid'] for x in BASE):
  raise RuntimeError('Raw stream does not contain exactly the frozen 27 strategy IDs')
 if any(t['exit']>SNAPSHOT or t['entry']>SNAPSHOT for t in raw):
  raise RuntimeError('Raw stream extends beyond frozen portfolio cutoff')
 if len({label(t) for t in raw})!=len(raw):raise RuntimeError('Duplicate strategy signal IDs in raw stream')
 # Cannot independently prove no omitted rejected signals from a CSV alone.
 if not truth(os.getenv('RAW_STREAM_COMPLETE','')):
  raise RuntimeError('Set RAW_STREAM_COMPLETE=yes ONLY after independently auditing every pre-p0, pre-nonhedging eligible signal; baseline parity alone cannot prove completeness')
 return raw

def package(reports):
 OUT.mkdir(parents=True,exist_ok=True)
 for name,rows in reports.items():export_csv(name,rows)
 with zipfile.ZipFile(BUNDLE,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
  for name in reports:z.write(OUT/(name+'.csv'),arcname=name+'.csv')

def run():
 global BASE,BASE_KEYS
 reports={}
 try:
  status('baseline',5,'Loading immutable portfolio 27 snapshot')
  BASE=load_base();BASE_KEYS={label(x) for x in BASE}
  if len(BASE_KEYS)!=3029:raise RuntimeError('Duplicate baseline signal identity')
  base_sim,_=equity(BASE)
  exp={'historical_cagr_pct':115.53348121349032,'max_closed_dd_pct':-17.089509091188603,
       'ending_balance_from_100':1731448888.7485507,'max_open_risk_floor_dd_pct':-17.84461250071128,'accepted_trades':3029,'strategy_ids':27}
  for field,val in exp.items():
   if not math.isclose(base_sim[field],val,rel_tol=1e-9,abs_tol=.00001):
    raise RuntimeError(f'Archived equity baseline parity FAILED {field}: {base_sim[field]} != {val}')
  reports['baseline_parity']=[{'check':k,'expected':v,'actual':base_sim[k],'result':'PASS'} for k,v in exp.items()]
  reports['baseline_coverage']=[{'snapshot':SNAPSHOT,'first_entry':min(x['entry'] for x in BASE),
     'last_exit':max(x['exit'] for x in BASE),'accepted_trades':len(BASE),
     'strategy_count':len({x['sid'] for x in BASE}),
     'accepted_archive_sha256':BASE_SHA256,'raw_signals_available':bool(RAW_PATH)}]
  status('diagnostic',30,'Calculating actual post-exit entry gaps and successor outcomes')
  detail,buckets,by_sid=post_exit_diagnostics(BASE)
  reports['successor_trades']=detail;reports['post_exit_gap_buckets']=buckets
  reports['by_strategy_successors']=by_sid
  removed=[];shadow_rows=[]
  for after in ('STOP','ANY'):
   for hrs in (1,2,4):
    retained,veto=shadow(BASE,hrs,after)
    removed.extend(veto)
    shadow_rows.append({'policy':after,'hours':hrs,
      'original_trades':len(BASE),'recorded_trades_vetoed':len(veto),
      'vetoed_winners':sum(x['r']>0 for x in veto),'vetoed_losers':sum(x['r']<0 for x in veto),
      'vetoed_r':sum(x['r'] for x in veto),'vetoed_weighted_r':sum(x['r']*(.75 if x['sid']=='EUR_JPY_M15_SHORT' else 1) for x in veto),
      'retained_recorded_trades':len(retained),
      'interpretation':'ACCEPTED_ONLY_DIAGNOSTIC_NOT_EXACT_COUNTERFACTUAL'})
  reports['accepted_only_shadow']=shadow_rows
  reports['accepted_only_shadow_vetoes']=removed
  reports['methodology']=[
   {'name':'baseline','value':'3029 accepted, 27 strategies, cutoff '+SNAPSHOT},
   {'name':'scope','value':'Offline read-only. Baseline stops inferred from historical r<0 fixed-stop outcome.'},
   {'name':'cooldown','value':'1/2/4 ELAPSED UTC HOURS after accepted stop or any exit, strict entry < exit + duration'},
   {'name':'H1/M15','value':'Clock-hour same duration across timeframes: M15=4/8/16 bars; H1=1/2/4 bars'},
   {'name':'shadow','value':'Accepted-only vetoes are descriptive and incomplete: missing previously suppressed eligible raw signals.'},
   {'name':'portfolio','value':'No portfolio score from accepted-only shadow. Exact replay requires complete historical raw eligible signals.'},
   {'name':'selection','value':'No parameter choosing from the six new live trades; frozen backtest is repeatedly inspected in-sample.'},
   {'name':'cost','value':'Existing historical 27-strategy cost assumptions only; doubled-cost full replay not inferred.'},
   {'name':'snapshot','value':'Sep 20 archive ends before the Sep 22-23 live stops; no live-trade contamination.'}]
  reports['exact_replay_readiness']=[{'state':'NOT_RUN','reason':'Missing complete 27-strategy raw eligible signals',
      'raw_file_env':'RAW_SIGNALS_FILE','raw_expected_columns':'sid,pair,side,tf,signal,entry,exit,r,rr,exit_reason',
      'complete_attestation':'RAW_STREAM_COMPLETE=yes after independent export audit',
      'baseline_parity':'MUST reproduce all 3029 accepted trade identities before any exact result is emitted',
      'note':'Baseline 3029 alone is insufficient for counterfactual eligibility'}]
  if RAW_PATH:
   status('raw',55,'Validating complete raw signal stream and unmodified control')
   raw=read_raw(RAW_PATH)
   control,events=exact_replay(raw,0,'STOP')
   expected=sorted(key(t) for t in BASE);actual=sorted(key(t) for t in control)
   if actual!=expected:
    a=Counter(actual);e=Counter(expected)
    raise RuntimeError('RAW 27 CONTROL PARITY FAILED: missing='+str(list((e-a).elements())[:3])+        ' extra='+str(list((a-e).elements())[:3])+        '. Do not interpret cooldown scenarios.')
   reports['exact_replay_readiness']=[{'state':'CONTROL_PASS','raw_rows':len(raw),
       'accepted_reproduced':len(control),'strategy_ids':len({x['sid'] for x in control}),
       'note':'Exact under user-attested complete raw stream; cannot prove omitted suppressed signals from baseline parity.'}]
   reports['raw_control_gate_summary']=[{'scenario':'CONTROL27','raw_signals':len(raw),
        **dict(Counter(x['decision'] for x in events)),**base_sim}]
   exact=[];delta=[];decisions=[];rolling_rows=[];lost=[]
   status('exact',65,'Chronological counterfactuals, no parameters changed')
   orig={label(t):t for t in control}
   for after in ('STOP','ANY'):
    for hrs in (1,2,4):
     tr,ev=exact_replay(raw,hrs,after)
     sim,eq=equity(tr)
     scen=f'{after}_{hrs}H'
     newer={label(t):t for t in tr}
     removed=[orig[k] for k in orig.keys()-newer.keys()]
     admitted=[newer[k] for k in newer.keys()-orig.keys()]
     exact.append({'scenario':scen,'raw_signals':len(raw),
       'blocked_cooldown':sum(x['decision']=='COOLDOWN' for x in ev),
       'blocked_opposite':sum(x['decision']=='OPPOSITE_PAIR_OPEN' for x in ev),
       'blocked_own_p0':sum(x['decision']=='OWN_STRATEGY_OPEN' for x in ev),
       'original_accepted_displaced':len(removed),'newly_eligible_accepted':len(admitted),**sim})
     delta.append({'scenario':scen,'delta_trades':len(tr)-len(BASE),
       'delta_weighted_r':sim['weighted_r']-base_sim['weighted_r'],
       'delta_cagr_percentage_points':sim['historical_cagr_pct']-base_sim['historical_cagr_pct'],
       'delta_closed_dd_percentage_points':sim['max_closed_dd_pct']-base_sim['max_closed_dd_pct'],
       'baseline_worst_dd_pct':base_sim['max_closed_dd_pct'],
       'scenario_worst_dd_pct':sim['max_closed_dd_pct'],
       'warning':'HISTORICAL_IN_SAMPLE_NOT_DEPLOYMENT'})
     decisions.extend(dict(v,scenario=scen) for v in ev)
     lost.extend({'scenario':scen,'change':'DISPLACED_OR_BLOCKED',**x} for x in removed)
     lost.extend({'scenario':scen,'change':'NEWLY_ELIGIBLE',**x} for x in admitted)
     rolling_rows.extend(rolling(eq,(12,24,36),scen))
   reports['exact_portfolio_scenarios']=exact
   reports['exact_deltas_vs_control27']=delta
   reports['exact_decision_events']=decisions
   reports['exact_trade_attribution']=lost
   reports['exact_rolling_12_24_36m']=rolling_rows
   reports['exact_rolling_summary']=[{'scenario':s,'months':m,
       'n':len(v),'worst_return_pct':min(x['return_pct'] for x in v) if v else '',
       'positive_pct':100*sum(x['return_pct']>0 for x in v)/len(v) if v else ''}
      for s in sorted({x['scenario'] for x in rolling_rows}) for m in (12,24,36)
      for v in [[x for x in rolling_rows if x['scenario']==s and x['months']==m]]]
  status('export',96,'Writing ZIP')
  package(reports)
  status('complete',100,'Descriptive cooldown diagnostic complete'+(' + baseline-gated exact replay' if RAW_PATH else '; exact replay gated: full raw stream unavailable'))
 except Exception as exc:
  reports['error']=[{'type':type(exc).__name__,'message':str(exc),'traceback':traceback.format_exc()}]
  try:package(reports)
  except Exception:pass
  with LOCK:STATE.update(state='error',message=f'{type(exc).__name__}: {exc}',traceback=traceback.format_exc())
  print(traceback.format_exc(),file=sys.stderr,flush=True)

if Flask:
 app=Flask(__name__)
 @app.get('/portfolio27-cooldown/status')
 def get_status():return jsonify(dict(STATE))
 @app.get('/portfolio27-cooldown/results')
 def get_results():
  if not BUNDLE.exists():return jsonify({'error':'ZIP not ready','status':STATE}),409
  return send_file(BUNDLE,as_attachment=True,download_name=BUNDLE.name)
 @app.get('/')
 def homepage():return jsonify({'service':'Frozen Portfolio 27 post-loss cooldown research',
    'status':'/portfolio27-cooldown/status','results':'/portfolio27-cooldown/results',
    'orders_supported':False,'trading_enabled':False})
if __name__=='__main__':
 if '--run-once' in sys.argv:
  run()
  if STATE['state']!='complete':sys.exit(1)
 else:
  if Flask is None:raise RuntimeError('Install flask, or run --run-once')
  threading.Thread(target=run,daemon=True,name='portfolio27-cooldown').start()
  app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),debug=False,use_reloader=False)
