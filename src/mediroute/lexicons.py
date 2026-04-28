"""Domain lexicons for rule-assisted IDP extraction.

The system intentionally combines LLM-ready prompts with deterministic lexicons.
For hackathon demo reliability, the app works even without a paid model endpoint.
"""

from __future__ import annotations

SPECIALTY_ALIASES: dict[str, list[str]] = {
    "internalMedicine": ["internal medicine", "medical ward", "physician", "general medicine"],
    "familyMedicine": ["family medicine", "primary care", "general practice", "gp clinic"],
    "pediatrics": ["pediatric", "paediatric", "children", "child health", "neonatal", "nicu"],
    "cardiology": ["cardiology", "cardiac", "heart", "ecg", "echo"],
    "generalSurgery": ["surgery", "surgical", "surgeon", "operating theatre", "operation theatre", "theatre"],
    "emergencyMedicine": ["emergency", "trauma", "accident and emergency", "a&e", "casualty"],
    "gynecologyAndObstetrics": ["obstetric", "ob/gyn", "gynaecology", "gynecology", "maternity", "labour", "labor ward", "c-section", "cesarean", "caesarean"],
    "orthopedicSurgery": ["orthopedic", "orthopaedic", "fracture", "bone surgery"],
    "dentistry": ["dentistry", "dental", "tooth", "oral health"],
    "ophthalmology": ["ophthalmology", "eye clinic", "cataract", "vision screening"],
}

PROCEDURE_ALIASES: dict[str, list[str]] = {
    "Performs C-section deliveries": ["c-section", "cesarean", "caesarean"],
    "Performs general surgical operations": ["general surgery", "surgical operations", "minor surgery", "major surgery"],
    "Provides hemodialysis treatment": ["dialysis", "hemodialysis", "haemodialysis"],
    "Provides ultrasound screening": ["ultrasound", "sonography"],
    "Provides X-ray imaging": ["x-ray", "xray", "radiography"],
    "Provides CT imaging": ["ct scan", "computed tomography"],
    "Provides emergency triage": ["triage", "emergency triage"],
    "Provides blood transfusion services": ["blood transfusion", "transfusion"],
    "Provides laboratory diagnostics": ["laboratory", "lab test", "diagnostic lab", "haematology", "hematology"],
    "Provides endoscopy procedures": ["endoscopy", "endoscopic"],
}

EQUIPMENT_ALIASES: dict[str, list[str]] = {
    "Has operating theatre": ["operating theatre", "operation theatre", "surgical theatre", "theatre"],
    "Has anesthesia equipment": ["anesthesia", "anaesthesia", "anaesthetic machine"],
    "Has blood bank": ["blood bank", "blood storage"],
    "Has oxygen supply": ["oxygen", "oxygen concentrator", "oxygen plant", "piped oxygen"],
    "Has patient monitors": ["patient monitor", "monitors", "cardiac monitor"],
    "Has ventilator support": ["ventilator", "mechanical ventilation"],
    "Has X-ray machine": ["x-ray", "xray", "radiography machine"],
    "Has ultrasound machine": ["ultrasound", "sonography machine"],
    "Has CT scanner": ["ct scanner", "ct scan", "computed tomography"],
    "Has laboratory analyzer": ["lab analyzer", "laboratory analyzer", "haematology analyzer", "hematology analyzer"],
    "Has dialysis machines": ["dialysis machine", "hemodialysis machine", "haemodialysis machine"],
    "Has backup power": ["backup generator", "generator", "solar backup", "backup power"],
    "Has ambulance": ["ambulance"],
}

CAPABILITY_ALIASES: dict[str, list[str]] = {
    "Provides emergency care": ["emergency", "trauma", "casualty", "a&e", "accident and emergency"],
    "Provides inpatient care": ["inpatient", "admission", "beds", "ward"],
    "Provides surgical care": ["surgery", "surgical", "operating theatre", "operation theatre"],
    "Provides ICU-level care": ["icu", "intensive care", "critical care"],
    "Provides NICU-level care": ["nicu", "neonatal intensive"],
    "Provides maternity care": ["maternity", "obstetric", "labour ward", "labor ward", "delivery"],
    "Provides pediatric care": ["pediatric", "paediatric", "children", "child health"],
    "Provides dialysis care": ["dialysis", "hemodialysis", "haemodialysis"],
    "Provides imaging diagnostics": ["x-ray", "ultrasound", "ct scan", "mri", "imaging"],
    "Provides laboratory diagnostics": ["laboratory", "diagnostic lab", "lab tests"],
}

CRITICAL_CAPABILITIES = [
    "Provides emergency care",
    "Provides surgical care",
    "Provides maternity care",
    "Provides ICU-level care",
    "Provides imaging diagnostics",
    "Provides laboratory diagnostics",
]

NEGATION_TERMS = [
    "no ",
    "not available",
    "without",
    "lacks",
    "lack of",
    "unavailable",
    "non-functional",
    "broken",
]
