"""Compare reviewed provenance while allowing exactly three resume-work counters."""
import copy
import json
from pathlib import Path
from pullback_lab import core as e

VERSION='RR_PROVENANCE_GUARD_V1_2026_10_25'
COUNTERS=('new_geometry_paths_computed','previous_paths_copied','previous_paths_verified')
REFERENCES=json.loads((Path(__file__).resolve().parent/'reviewed_provenance.json').read_bytes())['references']
for digest,record in REFERENCES.items():
    e.require(e.sha(e.canon(record['payload']))==digest,'Reviewed provenance payload checksum failed')


def strict_json(raw):
    def pairs(items):
        obj={}
        for key,value in items:
            if key in obj:raise ValueError('Duplicate JSON key in provenance: '+key)
            obj[key]=value
        return obj
    def constant(value):raise ValueError('Non-finite number in provenance')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)


def differences(expected,actual,prefix=''):
    if type(expected) is not type(actual):return [prefix or '<root>']
    if isinstance(expected,dict):
        result=[]
        for key in sorted(set(expected)|set(actual)):
            path=(prefix+'.' if prefix else '')+key
            if key not in expected or key not in actual:result.append(path)
            else:result.extend(differences(expected[key],actual[key],path))
            if len(result)>=12:break
        return result[:12]
    if isinstance(expected,list):
        if len(expected)!=len(actual):return [prefix+'.length']
        result=[]
        for i,(a,b) in enumerate(zip(expected,actual)):
            result.extend(differences(a,b,prefix+'['+str(i)+']'))
            if len(result)>=12:break
        return result[:12]
    return [] if expected==actual else [prefix or '<root>']


def verify_report(path,expected_sha256,expected_bytes=None):
    """Return the observed byte hash and optional narrowly scoped audit record.

    Non-provenance reports still require byte-for-byte parity. A mismatched
    provenance file MUST match a trusted reviewed payload in every field other
    than the three named nonnegative integer counters. No source bytes are edited.
    """
    path=Path(path);raw=path.read_bytes();observed=e.sha(raw)
    exact=observed==expected_sha256 and (expected_bytes is None or len(raw)==expected_bytes)
    if path.name!='source_provenance.json':
        e.require(exact,'Reviewed matrix summary changed: '+path.name)
        return observed,None
    reference=REFERENCES.get(expected_sha256)
    if reference is None:
        e.require(exact,'Unrecognized reviewed provenance digest; cannot safely compare '+path.name)
        return observed,None
    expected=reference['payload'];actual=strict_json(raw)
    e.require(isinstance(actual,dict),'Provenance must be a JSON object')
    locked_expected=copy.deepcopy(expected);locked_actual=copy.deepcopy(actual);changes=[]
    for name in COUNTERS:
        # Added or omitted fields are not covered by this exception.
        if name in expected:
            e.require(name in actual and type(actual[name]) is int and actual[name]>=0,
                      'Invalid or missing provenance work counter: '+name)
            e.require(type(expected[name]) is int and expected[name]>=0,'Invalid reviewed counter')
            if actual[name]!=expected[name]:changes.append(dict(field=name,reviewed=expected[name],observed=actual[name]))
            del locked_expected[name];del locked_actual[name]
    changed=differences(locked_expected,locked_actual)
    e.require(not changed,'Reviewed provenance changed in locked fields: '+', '.join(changed))
    stable_sha=e.sha(e.canon(locked_expected))
    return observed,dict(file=str(path),version=VERSION,reviewed_sha256=expected_sha256,
        observed_sha256=observed,stable_fields_sha256=stable_sha,
        classification='EXACT_BYTES' if exact else 'COUNTERS_ONLY' if changes else 'JSON_FORMATTING_ONLY',
        accepted_counter_changes=changes,locked_fields_match=True,source_file_modified=False)
