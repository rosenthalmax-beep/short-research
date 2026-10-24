"""Coarse RR comparison with six exact controls and unchanged GBP accounting."""
import csv
import fcntl
import gc
import gzip
import io
import json
import os
import shutil
import threading
from pathlib import Path
from pullback_lab import core as e
from pullback_lab.accounting import Replay
from pullback_lab.market import Market
from pullback_lab.funding import Funding
from pullback_lab.broker_reconcile import reconcile
from pullback_lab.paths import truncate
from pullback_lab.reports import windows
from audusd_matrix import io as x
from audusd_matrix.runner import Research as OriginalResearch
from .signals import generate_grid,qualification_parity
from audusd_matrix.reporting import augment,opportunity_rows,account_screen
from audusd_matrix.cache import MIB,StoragePause
from .config import VERSION,MODES,OVERLAPS,candidates,configuration
from .reuse import Previous,ReusePool,PINS,add_previous_sources,safe_file
from .reporting import regions,review_text,comparisons
from .download import write_archive

HERE=Path(__file__).resolve().parent
BASELINE=dict(candidate_id='BASELINE',kind='BASELINE',distance_max=None,body_min=None)


class Research(OriginalResearch):
    def __init__(self,environ=None):
        env=dict(os.environ if environ is None else environ)
        self.original_matrix_root=Path(env.get('AUDUSD_MATRIX_OUTPUT_DIR','/data/p32_audusd_long_matrix')).resolve()
        self.extension_root=Path(env.get('AUDUSD_EXTENSION_OUTPUT_DIR','/data/p32_audusd_long_extension')).resolve()
        self.lifetime_root=Path(env.get('AUDUSD_LIFETIME_OUTPUT_DIR','/data/p32_audusd_lifetime')).resolve()
        self.previous_root=Path(env.get('AUDUSD_LIFETIME_EXTENSION_OUTPUT_DIR','/data/p32_audusd_lifetime_extension')).resolve()
        self.previous_directory=self.previous_root/PINS['signature']
        directory=Path(env.get('AUDUSD_RR_OUTPUT_DIR','/data/p32_audusd_rr')).resolve()
        old_roots=[self.previous_root,self.lifetime_root,self.extension_root,Path(env.get('AUDUSD_MATRIX_OUTPUT_DIR','/data/p32_audusd_long_matrix')).resolve(),
                   Path(env.get('AUDUSD_FREQUENCY_OUTPUT_DIR','/data/p32_audusd_frequency')).resolve(),Path(env.get('PULLBACK_OUTPUT_DIR','/data/p32_pullback_pass1')).resolve(),
                   Path(env.get('PULLBACK_LEGACY_CACHE_DIR','/data/p32_signal_admission')).resolve(),
                   Path(env.get('AUDUSD_RECON_OUTPUT_DIR','/data/p32_audusd_reconciliation')).resolve(),
                   Path(env.get('AUDUSD_COVERAGE_OUTPUT_DIR','/data/p32_audusd_path_coverage')).resolve()]
        e.require(all(not directory.is_relative_to(p) and not p.is_relative_to(directory) for p in old_roots),
                  'Keep lifetime outputs separate from all existing research directories')
        mapped=dict(env,AUDUSD_MATRIX_OUTPUT_DIR=str(directory),
            AUDUSD_MATRIX_RAW_CACHE_MB=env.get('AUDUSD_RR_RAW_CACHE_MB','64'),
            AUDUSD_MATRIX_RESERVE_MB=env.get('AUDUSD_RR_RESERVE_MB','96'))
        super().__init__(mapped)
        # Verify original reference modules; the existing generator accepts the new fixed grid.
        root=HERE.parent
        for name,checksum in PINS['matrix_code'].items():
            path=root/name if name=='audusd_pullback_matrix.py' else root/'audusd_matrix'/name
            e.require(x.sha_file(path)==checksum,'Pinned original matrix code changed: '+name)
        for name,checksum in PINS['lifetime_code'].items():
            path=root/name if name=='audusd_pullback_lifetime.py' else root/'audusd_lifetime'/name
            e.require(x.sha_file(path)==checksum,'Pinned lifetime engine module changed: '+name)
        for name,checksum in PINS['lifetime_extension_code'].items():
            path=root/name if name=='audusd_pullback_lifetime_extension.py' else root/'audusd_lifetime_extension'/name
            e.require(x.sha_file(path)==checksum,'Pinned lifetime-extension module changed: '+name)
        self.protocol=configuration()
        files=sorted(HERE.glob('*.py'))+sorted(HERE.glob('*.json'))+[root/'audusd_pullback_rr.py']
        self.proof={p.name:x.sha_file(p) for p in files}
        self.signature=e.sha(e.canon(dict(protocol=self.protocol,extension_code=self.proof,engine=x.ENGINE,
                                         original_matrix_code=PINS['matrix_code'],prior_lifetime_code=PINS['lifetime_code'],prior_extension_code=PINS['lifetime_extension_code'])))
        self.work=self.directory/self.signature;self.previous=None;self.parity={}
        self.state.update(version=VERSION,signature=self.signature,candidates_required=18,accounts_required=252,
                          overlap_candidates_required=6,overlap_account_payloads_required=84,
                          overlap_account_payloads_matched=0,raw_cache_limit_bytes=self.raw_mb*MIB)

    def make_store(self):
        store=super().make_store()
        try:
            add_previous_sources(store,self.previous,self.previous_root/'canonical_candles/candles.sqlite3')
            for ancestor,cache in self.previous.ancestors(self.original_matrix_root,self.extension_root,self.lifetime_root):
                add_previous_sources(store,ancestor,cache)
        except Exception:store.close();raise
        return store

    def save_parity(self):
        rows=[self.parity[cid] for cid in OVERLAPS if cid in self.parity]
        total=sum(len(r.get('accounts',[])) for r in rows)
        x.atomic(self.work/'overlap_parity.json',e.canon(dict(
            status='PASS' if len(rows)==6 and total==84 else 'IN_PROGRESS',controls=rows,
            signal_sets_required=6,account_payloads_required=84,account_payloads_matched=total,
            previous_signature=PINS['signature'],comparison='CANONICAL_FULL_PAYLOAD_EQUALITY; NOT_JUST_HEADLINE_STATS')))
        self.progress(overlap_account_payloads_matched=total)

    def signal_parity(self,cid,signals):
        if cid not in OVERLAPS:return
        old=self.previous.controls[cid]['signals']
        e.require(e.canon(signals)==e.canon(old),'Overlap signals differ from previous matrix: '+cid)
        self.parity[cid]=dict(candidate_id=cid,signals='PASS',signal_count=len(signals),opportunities='PENDING',accounts=[])

    def opportunity_parity(self,cid,opps):
        if cid not in OVERLAPS:return
        e.require(e.canon(opps)==e.canon(self.previous.controls[cid]['opportunities']),
                  'Overlap opportunity records differ from previous matrix: '+cid)
        self.parity[cid]['opportunities']='PASS';self.save_parity()

    def account_parity(self,cid,window,mode,result):
        if cid not in OVERLAPS:return
        old=self.previous.checkpoint(cid,window,mode)
        new_sha,old_sha=e.sha(e.canon(result)),e.sha(e.canon(old))
        e.require(new_sha==old_sha,'Overlap account payload differs from previous matrix: '+cid+'/'+window+'/'+mode)
        self.parity[cid]['accounts'].append(dict(window_id=window,financing_mode=mode,status='PASS',result_sha256=new_sha))
        self.save_parity()

    def run(self):
        resources=[];own_lock=None;prior_lock=None;matrix_lock=None;extension_lock=None;lifetime_lock=None
        try:
            self.directory.mkdir(parents=True,exist_ok=True);own_lock=(self.directory/'runner.lock').open('ab')
            try:fcntl.flock(own_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:raise ValueError('Another lifetime-matrix process is running')
            prior_lock=safe_file(self.previous_root,'runner.lock').open('rb')
            try:fcntl.flock(prior_lock,fcntl.LOCK_SH|fcntl.LOCK_NB)
            except BlockingIOError:raise ValueError('Previous matrix writer is still running; stop it before lifetime replay')
            matrix_lock=safe_file(self.original_matrix_root,'runner.lock').open('rb')
            try:fcntl.flock(matrix_lock,fcntl.LOCK_SH|fcntl.LOCK_NB)
            except BlockingIOError:raise ValueError('Original matrix writer is still running; stop it before lifetime replay')
            extension_lock=safe_file(self.extension_root,'runner.lock').open('rb')
            try:fcntl.flock(extension_lock,fcntl.LOCK_SH|fcntl.LOCK_NB)
            except BlockingIOError:raise ValueError('Older extension writer is still running; stop it before replay')
            lifetime_lock=safe_file(self.lifetime_root,'runner.lock').open('rb')
            try:fcntl.flock(lifetime_lock,fcntl.LOCK_SH|fcntl.LOCK_NB)
            except BlockingIOError:raise ValueError('Prior lifetime writer is still running; stop it before RR replay')
            e.require(shutil.disk_usage(self.directory).free>=self.reserve,'Keep the lifetime-matrix reserve free; old research files were not changed')
            from pullback_lab.runner import persistence
            e.require(persistence(self.directory,create=True)['separate_mount_detected'],'Keep AUDUSD_RR_OUTPUT_DIR on mounted /data')
            self.work.mkdir(parents=True,exist_ok=True)
            (self.work/'ERROR.json').unlink(missing_ok=True)
            x.atomic(self.work/'protocol.json',e.canon(dict(signature=self.signature,**self.protocol)))
            x.atomic(self.work/'study_context.json',(HERE/'study_context.json').read_bytes())
            self.progress(status='RUNNING',phase='VERIFY_PREVIOUS_MATRIX',started_at_utc=e.stamp(e.datetime.now(e.UTC)))
            self.previous=Previous(self.previous_directory,self.progress)
            witness=self.previous.proof();saved=self.work/'previous_matrix_witness.json'
            if saved.is_file():e.require(e.canon(json.loads(saved.read_bytes()))==e.canon(witness),'Previous source witness differs from lifetime checkpoint')
            x.atomic(saved,e.canon(witness))
            source,coverage,original=self.prepare_source()
            evidence=json.loads((HERE.parent/'pullback_lab/saved_broker_evidence.json').read_bytes())
            broker,checks=reconcile(evidence);e.require(broker['status']=='PASS','Saved GBP broker reconciliation failed')
            x.atomic(self.work/'broker_reconciliation.json',e.canon(dict(summary=broker,checks=checks)))
            store=self.make_store();resources.append(store)
            m15,h1,ready,history=self.load_history(store,source)
            e.require(e.stamp(ready)==coverage['eligibility']['effective_evaluation_start_utc'],'Warm-up readiness differs from frozen source')
            signals,counts=generate_grid(m15,h1,source['start'],source['end'],ready,self.progress,rules=[BASELINE]+candidates())
            x.atomic(self.work/'baseline_parity.json',e.canon(x.baseline_parity(signals['BASELINE'],original)))
            x.atomic(self.work/'qualification_sequence_parity.json',e.canon(qualification_parity(signals)))
            del original,h1;gc.collect()
            pool=ReusePool(self.work,store,m15,'AUD_USD',e.dt(source['end']),x.PINS['signature'],self.progress,previous=self.previous)
            resources.append(pool);self.pool=pool
            seeded=pool.seed_source(self.source/'pairs/AUD_USD',signals['BASELINE'],self.progress)
            provenance=dict(source_pins=x.PINS,engine_pins=x.ENGINE,extension_code=self.proof,
                original_matrix_code=PINS['matrix_code'],history=history,path_reuse=seeded,
                previous_matrix_signature=PINS['signature'],all_history_seen=True,
                source_files_modified=False,historical_broker_terms_reconstructed=False,
                coverage_audit_candles_used=False)
            x.atomic(self.work/'source_provenance.json',e.canon(provenance))
            self.process_candidates(signals,counts,pool,store,evidence,broker,source,ready)
            e.require(len(self.parity)==6 and sum(len(p['accounts']) for p in self.parity.values())==84,'All six RR3 controls must match all 84 accounts')
            self.previous.assert_unchanged()
            provenance['previous_paths_verified']=pool.previous_paths_verified
            provenance['previous_paths_copied']=pool.previous_paths_copied
            provenance['new_geometry_paths_computed']=pool.computed
            x.atomic(self.work/'source_provenance.json',e.canon(provenance))
            x.atomic(self.work/'private_source_index.json.gz',gzip.compress(e.canon(store.index())))
            self.progress(phase='FINALIZE_REPORTS',candidates_completed=18)
            self.publish(complete=True)
            self.progress(status='COMPLETE',phase='COMPLETE',candidates_completed=18,accounts_completed=252,overlap_account_payloads_matched=84)
        except Exception as exc:
            safe=str(exc) if isinstance(exc,(ValueError,e.CandleFetchError,FileNotFoundError)) else type(exc).__name__
            self.state.update(status='PAUSED_STORAGE' if isinstance(exc,StoragePause) else 'ERROR',phase='STOPPED',error=safe)
            if self.work.is_dir():
                try:
                    x.atomic(self.work/'ERROR.json',e.canon(dict(error=safe,type=type(exc).__name__)))
                    x.atomic(self.work/'status.json',e.canon(self.state));self.publish(complete=False)
                except Exception:pass
        finally:
            for resource in reversed(resources):
                try:resource.close()
                except Exception:pass
            if lifetime_lock:lifetime_lock.close()
            if extension_lock:extension_lock.close()
            if matrix_lock:matrix_lock.close()
            if prior_lock:prior_lock.close()
            if own_lock:own_lock.close()

    def process_candidates(self,signals,counts,pool,store,evidence,broker,source,ready):
        funding=Funding(evidence['funding_schedules']);markup=broker['observed_symmetric_conversion_factor_markup']
        term=next(i for i in evidence['instruments'] if i['name']=='AUD_USD')
        ws=[dict(window_id='FULL_HISTORY',start=source['start'],end=source['end'])]+windows(source['start'],source['end'])
        e.require(len(ws)==7,'Expected seven frozen account windows')
        total=0;done=0
        for rule in candidates():
            cid=rule['candidate_id'];directory=self.work/'candidates'/cid;directory.mkdir(parents=True,exist_ok=True)
            selected=signals[cid];self.signal_parity(cid,selected)
            if self.verify_complete(directory):
                if cid in OVERLAPS:
                    with gzip.open(directory/'opportunities.jsonl.gz','rb') as f:self.opportunity_parity(cid,[json.loads(l) for l in f])
                    for window in ws:
                        for mode in MODES:
                            cp=json.loads(gzip.decompress((directory/'accounts'/(window['window_id']+'__'+mode+'.json.gz')).read_bytes()))
                            e.require(cp['signature']==self.signature and e.sha(e.canon(cp['result']))==cp['sha256'],'Completed overlap checkpoint changed')
                            self.account_parity(cid,window['window_id'],mode,cp['result'])
                done+=1;total+=14;self.progress(candidates_completed=done,accounts_completed=total,phase='REUSE_COMPLETE_CANDIDATE',candidate_id=cid);continue
            self.progress(phase='PRICE_CANDIDATE',candidate_id=cid,candidate_number=done+1,qualifications_required=len(selected))
            x.write_jsonl(directory/'signals.jsonl.gz',selected);x.atomic(directory/'generation_counts.json',e.canon(counts[cid]))
            market=Market(store,self.progress);opps=[]
            for n,s in enumerate(selected):
                p=pool.get(s);shadow=pool.shadow(s,p,market,funding,term,markup)
                opps.append(dict(signal_key=s['signal_key'],pair='AUD_USD',side='BUY',control=s['control'],entry_utc=s['entry_utc'],features=s['features'],
                    path_cache_key=p['path_cache_key'],path_status=p['status'],shadow=shadow,counterfactual_outcome_status=p.get('counterfactual_outcome_status'),
                    counterfactual_price_r=p.get('observed_path_price_r') if p['status']=='ENTRY_PROXY_REJECTED' else None))
                if n%50==0:self.progress(phase='PRICE_CANDIDATE',candidate_id=cid,qualifications_processed=n+1,
                    native_paths_computed=pool.computed,path_cache_hits=pool.cached,previous_paths_copied=pool.previous_paths_copied)
            self.opportunity_parity(cid,opps)
            x.write_jsonl(directory/'opportunities.jsonl.gz',opps)
            x.atomic(directory/'opportunity_summaries.csv',x.csv_bytes(opportunity_rows(rule,opps,source['start'],source['end'],e.stamp(ready))))
            del opps;gc.collect();summaries=[]
            for window in ws:
                lo=max(e.dt(window['start']),ready);hi=e.dt(window['end'])
                subset=[s for s in selected if lo<=e.dt(s['entry_utc'])<hi]
                for mode in MODES:
                    path=directory/'accounts'/(window['window_id']+'__'+mode+'.json.gz')
                    if path.is_file():
                        cp=json.loads(gzip.decompress(path.read_bytes()))
                        e.require(cp['signature']==self.signature and e.sha(e.canon(cp['result']))==cp['sha256'],'Account checkpoint signature/checksum failed')
                        result=cp['result']
                    else:
                        self.progress(phase='REPLAY_GBP_ACCOUNT',candidate_id=cid,window_id=window['window_id'],funding_mode=mode)
                        result=Replay(subset,lambda s:truncate(pool.get(s),hi),Market(store,self.progress),funding,evidence['instruments'],
                            'END_ASC',mode,markup,initial=100.,start=lo,end=hi,progress=self.progress).run()
                        e.require(result['summary']['status']=='COMPLETE_CONDITIONAL','Account stopped: '+str(result['summary']['stopped_reason']))
                        augment(result,lo,hi)
                        # A parity failure is not saved as a valid checkpoint.
                        self.account_parity(cid,window['window_id'],mode,result)
                        x.atomic(path,gzip.compress(e.canon(dict(signature=self.signature,result=result,sha256=e.sha(e.canon(result))))))
                    if path.is_file() and cid in OVERLAPS and not any(r['window_id']==window['window_id'] and r['financing_mode']==mode for r in self.parity[cid]['accounts']):
                        self.account_parity(cid,window['window_id'],mode,result)
                    summary=augment(result,lo,hi)
                    summaries.append(dict(**rule,window_id=window['window_id'],requested_start=window['start'],effective_start=e.stamp(lo),end=window['end'],
                        complete_requested_window=lo==e.dt(window['start']),scope='ONE_CANDIDATE_AUDUSD_LONG_GBP100_NOT_LIVE32_COMBINED',**summary))
                    total+=1;self.progress(accounts_completed=total)
            x.atomic(directory/'account_summaries.json',e.canon(summaries))
            files={p.relative_to(directory).as_posix():x.sha_file(p) for p in directory.rglob('*') if p.is_file() and not p.name.endswith('.tmp') and p.name!='COMPLETE.json'}
            x.atomic(directory/'COMPLETE.json',e.canon(dict(signature=self.signature,candidate_id=cid,accounts=14,files=files)))
            done+=1;self.progress(candidates_completed=done,phase='CANDIDATE_COMPLETE',candidate_id=cid)
            self.publish(complete=False)

    def publish(self,complete=False):
        dirs=[self.work/'candidates'/r['candidate_id'] for r in candidates() if (self.work/'candidates'/r['candidate_id']/'COMPLETE.json').is_file()]
        accounts=[];opps=[]
        for directory in dirs:
            accounts+=json.loads((directory/'account_summaries.json').read_bytes())
            opps+=list(csv.DictReader(io.StringIO((directory/'opportunity_summaries.csv').read_text())))
        screens=[dict(**r,**account_screen([a for a in accounts if a['candidate_id']==r['candidate_id']])) for r in candidates()
                 if any(a['candidate_id']==r['candidate_id'] for a in accounts)]
        blocks=regions(candidates(),accounts)
        paired=comparisons(accounts)
        if complete:e.require(len(accounts)==252 and len(dirs)==18 and all(p['opportunities']=='PASS' and len(p['accounts'])==14 for p in self.parity.values()),'Incomplete final lifetime matrix')
        text=review_text(len(dirs),len(accounts))
        pool=getattr(self,'pool',None)
        if pool is not None:
            def path_rows():
                for key,body,checksum in pool.db.execute('SELECT key,body,sha256 FROM paths ORDER BY key'):
                    e.require(e.sha(body)==checksum,'Private shared-path checksum failed while exporting');yield json.loads(body)
            x.write_jsonl(self.work/'unique_price_paths.jsonl.gz',path_rows())
        with self.lock:
            for name,rows in [('account_matrix.csv',accounts),('opportunity_matrix.csv',opps),('candidate_screens.csv',screens),('plateau_regions.csv',blocks),('control_comparisons.csv',paired)]:
                x.atomic(self.work/name,x.csv_bytes(rows))
            x.atomic(self.work/'REVIEW.md',text.encode())
            x.atomic(self.work/'run_state.json',e.canon(dict(status='COMPLETE_CONDITIONAL_RR_MATRIX' if complete else 'PARTIAL',signature=self.signature,
                published_candidates=[p.name for p in dirs],candidates_completed=len(dirs),candidates_required=18,
                accounts_completed=len(accounts),accounts_required=252,plateau_regions_passing=sum(r['conditional_plateau_screen_pass'] for r in blocks),
                overlap_account_payloads_matched=sum(len(r['accounts']) for r in self.parity.values()),
                automatic_selection=False,full_portfolio_validated=False)))
        target=self.work/'AUDUSD_RR_SUMMARY.zip';temp=target.with_suffix('.zip.tmp')
        with temp.open('wb') as f:write_archive(self,f,summary=True)
        e.require(temp.stat().st_size<20_000_000,'Summary unexpectedly exceeds 20 MB; completed evidence kept')
        temp.replace(target);self.state.update(result_ready=True,result='/results-summary.zip',full_result='/results.zip')
