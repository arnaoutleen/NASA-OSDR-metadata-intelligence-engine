"""
NASA OSDR Metadata Intelligence Engine - Data Retriever

Orchestrates retrieval of unified sample records from NASA OSDR by combining
metadata from the Biodata API, Developer API, ISA-Tab parsing, and the
GeneLab files API.

Each sample is represented as a :class:`SampleRecord` that captures the
full picture of available data: organism, organ, all assay types, platforms,
and associated data files.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.core.constants import CONTROLLED_ASSAY_TYPES, CONTROLLED_TISSUES
from src.core.isa_parser import ISAParser, ISAStudyMetadata
from src.core.mission_resolver import MissionResolver
from src.core.osdr_client import OSDRClient


@dataclass
class SampleRecord:
    """Unified record for a single sample across all data sources."""

    osd: str
    source_name: str
    sample_name: str
    organism: str = ""
    material_type: str = ""
    measurement_types: List[str] = field(default_factory=list)
    technology_types: List[str] = field(default_factory=list)
    device_platforms: List[str] = field(default_factory=list)
    data_files: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "osd": self.osd,
            "source_name": self.source_name,
            "sample_name": self.sample_name,
            "organism": self.organism,
            "material_type": self.material_type,
            "measurement_types": self.measurement_types,
            "technology_types": self.technology_types,
            "device_platforms": self.device_platforms,
            "data_files": self.data_files,
        }


def _normalize_tissue(value: str) -> str:
    """Normalize a tissue/organ name using controlled vocabulary."""
    if not value:
        return ""
    lookup = value.lower().strip()
    return CONTROLLED_TISSUES.get(lookup, value.strip())


def _normalize_assay_type(value: str) -> str:
    """Normalize an assay/technology type using controlled vocabulary."""
    if not value:
        return ""
    lookup = value.lower().strip()
    return CONTROLLED_ASSAY_TYPES.get(lookup, value.strip())


class DataRetriever:
    """
    Retrieves unified :class:`SampleRecord` objects for OSD studies.

    Combines data from:
    - ``OSDRClient.fetch_study_json()``  (organism, material_type, samples)
    - ``ISAParser.parse()``              (per-sample assay details + data files)
    - ``OSDRClient.fetch_files_list()``  (secondary file association)
    """

    def __init__(
        self,
        client: OSDRClient,
        parser: ISAParser,
        resolver: MissionResolver,
    ):
        self._client = client
        self._parser = parser
        self._resolver = resolver

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def retrieve_osd(self, osd_id: str) -> List[SampleRecord]:
        """
        Retrieve all sample records for a single OSD study.

        1. Fetch study-level metadata (organism, material_type)
        2. Download + parse ISA-Tab (sample details, assay details, data files)
        3. For each unique sample, aggregate across all assays it appears in
        """
        osd_id = OSDRClient.normalize_osd_id(osd_id)

        # Fetch study-level metadata via the API
        study_json = self._client.fetch_study_json(osd_id)
        organism = ""
        study_material_type = ""
        if study_json:
            organism = study_json.get("organism", "")
            study_material_type = study_json.get("material_type", "")

        # Download and parse ISA-Tab
        self._client.download_isa_tab(osd_id)
        isa_metadata = self._parser.parse(osd_id)

        if not isa_metadata or not isa_metadata.samples:
            return []

        # Build per-sample assay aggregation from ISA-Tab assays
        sample_assay_info = self._aggregate_assay_info(isa_metadata)

        # Build a secondary file-to-sample map from the GeneLab files API
        # in case ISA-Tab data_file columns are empty
        secondary_files = self._build_secondary_file_map(osd_id, isa_metadata)

        records: List[SampleRecord] = []
        for isa_sample in isa_metadata.samples:
            name = isa_sample.sample_name
            info = sample_assay_info.get(name, {})

            # Determine material_type (organ) from sample characteristics first
            material = _normalize_tissue(
                isa_sample._get_characteristic("organism part")
                or isa_sample._get_characteristic("material type")
                or study_material_type
            )

            # Determine organism from sample characteristics, fall back to study-level
            sample_organism = (
                isa_sample._get_characteristic("organism") or organism
            )

            # Collect data files: primary from ISA-Tab, secondary from file listing
            all_files = list(info.get("data_files", []))
            if not all_files:
                all_files = secondary_files.get(name, [])

            record = SampleRecord(
                osd=osd_id,
                source_name=isa_sample.source_name,
                sample_name=name,
                organism=sample_organism,
                material_type=material,
                measurement_types=sorted(set(info.get("measurement_types", []))),
                technology_types=sorted(set(info.get("technology_types", []))),
                device_platforms=sorted(set(info.get("device_platforms", []))),
                data_files=all_files,
            )
            records.append(record)

        return records

    def retrieve_osds(self, osd_ids: List[str]) -> List[SampleRecord]:
        """Retrieve sample records for multiple OSD studies."""
        all_records: List[SampleRecord] = []
        for osd_id in osd_ids:
            all_records.extend(self.retrieve_osd(osd_id))
        return all_records

    def retrieve_mission(self, mission_name: str) -> List[SampleRecord]:
        """Retrieve sample records for all OSDs in a mission."""
        osd_ids = self._resolver.resolve_mission(mission_name)
        if not osd_ids:
            return []
        return self.retrieve_osds(osd_ids)

    def retrieve_all(self) -> List[SampleRecord]:
        """Retrieve sample records for all known studies."""
        osd_ids = self._client.list_all_study_ids()
        return self.retrieve_osds(osd_ids)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate_assay_info(
        isa_metadata: ISAStudyMetadata,
    ) -> Dict[str, Dict[str, Any]]:
        """
        For each sample, collect measurement_types, technology_types,
        device_platforms, and data_files across ALL assays it appears in.
        """
        info: Dict[str, Dict[str, Any]] = {}

        for assay in isa_metadata.assays:
            m_type = _normalize_assay_type(assay.measurement_type)
            t_type = _normalize_assay_type(assay.technology_type)
            platform = assay.technology_platform or ""

            for sample_detail in assay.sample_details:
                name = sample_detail.sample_name
                if name not in info:
                    info[name] = {
                        "measurement_types": [],
                        "technology_types": [],
                        "device_platforms": [],
                        "data_files": [],
                    }

                entry = info[name]
                if m_type and m_type not in entry["measurement_types"]:
                    entry["measurement_types"].append(m_type)
                if t_type and t_type not in entry["technology_types"]:
                    entry["technology_types"].append(t_type)
                if platform and platform not in entry["device_platforms"]:
                    entry["device_platforms"].append(platform)

                for f in sample_detail.data_files:
                    if f not in entry["data_files"]:
                        entry["data_files"].append(f)

        return info

    def _build_secondary_file_map(
        self,
        osd_id: str,
        isa_metadata: ISAStudyMetadata,
    ) -> Dict[str, List[str]]:
        """
        Build a sample_name -> [filenames] map by matching filenames
        from the GeneLab files API against known sample names.
        Only used when ISA-Tab data_file columns are empty.
        """
        has_isa_files = any(
            detail.data_files
            for assay in isa_metadata.assays
            for detail in assay.sample_details
        )
        if has_isa_files:
            return {}

        files_list = self._client.fetch_files_list(osd_id)
        if not files_list:
            return {}

        sample_names = {s.sample_name for s in isa_metadata.samples}
        result: Dict[str, List[str]] = {n: [] for n in sample_names}

        for file_entry in files_list:
            fname = file_entry.get("file_name", "")
            if not fname:
                continue
            for sname in sample_names:
                if sname in fname:
                    result[sname].append(fname)

        return result
