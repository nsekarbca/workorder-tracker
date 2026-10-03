"""
Starting client list (Facility No | Client Name), loaded once into the
`clients` table the first time the app starts with that table empty. After
that the list is managed from Admin -> Clients and this file is not used.
"""

_RAW = """
120|Commonwealth Emergency Physicians PC
131|VietMed LLC
147|Crestview Urgent Care Inc
161|Oracle Health Systems Inc
164|Central Emergency Physicians PSC
165|Westmoreland Em Med Specialists PC
190|Madison Emergency Physicians SC
206|Sussex Emergency Associates, LLC
304|Emergency Med Assoc of McMinnville
310|Bessemer Emergency Physicians LLC
345|Southern Wisconsin Emerg Assoc SC
347|Illinois Emerg Med Specialists LLC
356|Haseeb Jabbar MD PA
416|Tufts Medical Center EP LLC
417|Emerson Emergency Physicians LLC
440|Texas Emergency Group LLC
441|Alaska Emergency Med Associates
442|Gregory P Clifford MD PC
460|Louisville Emerg Med Associates PSC
490|Abington Emergency Phys Assoc PC
519|Northeast Emerg Med Specialists LLC
528|Chesapeake Emergency Physicians
530|Emergency Medicine Care LLC
1012|Summit Emergency Medicine
1014|Emergency Physician Associates, PA
1043|Mohammad Farsakh MD, PA
1051|Taylor Made Medical Care, LLC
1073|MEMORIAL HERMANN EMERGENCY PHYSICIANS
1112|Bethesda Emergency Associates LLC
1114|Beaumont Emergency Med Assoc, PLLC
1120|Valley View Emergency Phys, LLC
1130|East Jefferson Emergency Mngmt, LLC
1172|Georgia Emerg Med Specialists, PC
1175|Oahu Emergency Phys Services, LLC
1182|Mat-Su Emergency Medicine Phys Corp
1191|East Central Iowa Acute Care, PLLC
1205|Louisville Observation Medicine Assoc
1228|Ascension Emergency Physicians, LLC
1243|Professional Emergency Physician Association, LLC
1264|South Miami Inpatient Phys, Inc.
1272|Harish Kotipoyina MD, PLLC
1277|WH Services Austin
1294|East Texas Medical Services PLLC
1295|ER Stat, Inc.
1296|WH Services Dallas
1302|WH Services Brewton
1304|Blue Radiology Services LLC
1311|Elite Hospital Partners
1312|EM Alliance
1320|Ascentist Physicians Group
1323|WH Services Atmore
1326|ChristianaCare Emergency Physicians
1330|RH Emergency Med of Caldwell
1333|Omni Emergency Medicine LLC
1336|Johnson County Emergency Medicine
1337|Linn County Emergency Medicine
1340|Rocky Mtn Emergency Specialists
1341|DEMI Healthcare Partners LLC
1343|Fountain Valley Emergency Phys
1346|Mt. Rainier Emergency Physicians
1347|Western Washington Emergency Phys
1351|Emergency Care Consultants
1359|Childrens Hospital
1370|Northeast Emergency Associates
1372|Eastern Carolina Emergency Physicians
1377|SWEA Fort
1378|LSU Healthcare Network
1380|New Orleans Physician Services Inc.
1391|Dubuque Emergency Physicians
1407|Denali Emergency Medicine Assoc
1408|Hawaiian Island Clinics
1413|Cobre Valley Emergency Services LLC
1416|Missouri Emergency Medicine LLC
1566|Northwest Acute Care Specialists PC
2250|Electric City Emergency Physicians
2259|Progressive Emerg Physicians PLLC
2267|Ohio Emerg Medicine Services
2284|HHA Hospital Medicine
2293|Elite Hospital Partners
2296|EHP of Ponca City
2297|Emerald Coast Management Solutions
2319|Chesapeake Obs Medicine Spec PC
100087|Bay Area Emergency Physicians LLC
100091|Presbyterian Healthcare Services
100092|Presbyterian Healthcare Services
100093|Presbyterian Healthcare Services
100119|Shenandoah Emergency Medicine Specialists, LLC
100125|Appleton Emergency Service
"""

SEED_CLIENTS = [
    (no.strip(), name.strip())
    for no, name in (line.split("|", 1) for line in _RAW.strip().splitlines())
]
