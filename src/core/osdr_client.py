"""
NASA OSDR Metadata Intelligence Engine - OSDR API Client

This module provides a client for fetching metadata from NASA's Open Science
Data Repository (OSDR) APIs and downloading ISA-Tab archives.

Data sources:
1. OSDR Biological Data API (Primary): https://visualization.osdr.nasa.gov/biodata/api/v2/
2. OSDR Developer API (Fallback): https://osdr.nasa.gov/osdr/data/
3. ISA-Tab Archives: Downloaded from GeneLab files API

All API responses are cached locally to minimize network requests.
"""

import io
import json
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

import requests


@dataclass
class APIConfig:
    """Configuration for OSDR API endpoints."""
    biodata_api_base: str = "https://visualization.osdr.nasa.gov/biodata/api/v2"
    developer_api_base: str = "https://osdr.nasa.gov/osdr/data"
    genelab_files_api: str = "https://genelab-data.ndc.nasa.gov/genelab/data/glds/files"
    osdr_download_base: str = "https://osdr.nasa.gov"
    request_timeout: int = 60


class OSDRClient:
    """
    Client for NASA OSDR APIs with caching and ISA-Tab download support.
    
    This client implements a priority-based fetching strategy:
    1. Check local cache first
    2. Try OSDR Biological Data API (structured JSON with characteristics/factorValues)
    3. Fall back to Developer API (study-level metadata)
    4. Download and parse ISA-Tab as last resort
    
    All successful fetches are cached locally for reproducibility.
    """
    
    def __init__(
        self,
        cache_dir: Path,
        isa_tab_dir: Path,
        config: Optional[APIConfig] = None,
    ):
        """
        Initialize OSDR client.
        
        Args:
            cache_dir: Directory for caching API JSON responses
            isa_tab_dir: Directory for storing downloaded ISA-Tab archives
            config: API configuration (uses defaults if not provided)
        """
        self.cache_dir = cache_dir
        self.isa_tab_dir = isa_tab_dir
        self.config = config or APIConfig()
        
        # Ensure directories exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.isa_tab_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize ISA parser for automatic ISA-Tab parsing
        # Import here to avoid circular imports
        from src.core.isa_parser import ISAParser
        self._isa_parser = ISAParser(isa_tab_dir)
    
    # =========================================================================
    # OSD ID Normalization
    # =========================================================================
    
    @staticmethod
    def normalize_osd_id(osd_id: str) -> str:
        """
        Normalize OSD ID to standard format (e.g., 'OSD-202').
        
        Handles various input formats:
        - "202" -> "OSD-202"
        - "OSD202" -> "OSD-202"
        - "osd-202" -> "OSD-202"
        
        Args:
            osd_id: The OSD identifier in any format
            
        Returns:
            Normalized OSD ID string
        """
        osd_id = str(osd_id).upper().strip()
        osd_id = osd_id.replace("OSD-", "").replace("OSD", "")
        return f"OSD-{osd_id}"
    
    @staticmethod
    def get_numeric_id(osd_id: str) -> str:
        """Extract numeric part from OSD ID (e.g., 'OSD-202' -> '202')."""
        osd_id = OSDRClient.normalize_osd_id(osd_id)
        return osd_id.replace("OSD-", "")
    
    # =========================================================================
    # Caching
    # =========================================================================
    
    def _get_cache_path(self, osd_id: str) -> Path:
        """Get the cache file path for a given OSD ID."""
        osd_id = self.normalize_osd_id(osd_id)
        return self.cache_dir / f"{osd_id}.json"
    
    def _load_from_cache(self, osd_id: str) -> Optional[Dict[str, Any]]:
        """
        Load cached JSON if it exists and contains valid sample data.
        
        Args:
            osd_id: The OSD identifier
            
        Returns:
            Cached metadata dict, or None if cache miss or invalid
        """
        cache_path = self._get_cache_path(osd_id)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            # Validate cache has samples
            if data.get("samples") and len(data.get("samples", [])) > 0:
                return data
            
            return None  # Cache exists but has no samples - refetch
            
        except (json.JSONDecodeError, IOError):
            return None
    
    def _save_to_cache(self, osd_id: str, data: Dict[str, Any]) -> None:
        """Save API response to cache."""
        cache_path = self._get_cache_path(osd_id)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    
    def clear_cache(self) -> None:
        """Clear all cached metadata files."""
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()
    
    # =========================================================================
    # Biodata API (Primary)
    # =========================================================================
    
    def _fetch_biodata_dataset(self, osd_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch dataset metadata from OSDR Biological Data API.
        
        Endpoint: /v2/dataset/{OSD-ID}/?format=json
        
        Args:
            osd_id: The OSD identifier
            
        Returns:
            API response as dict, or None on failure
        """
        osd_id = self.normalize_osd_id(osd_id)
        url = f"{self.config.biodata_api_base}/dataset/{osd_id}/?format=json"
        
        try:
            response = requests.get(url, timeout=self.config.request_timeout)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        
        return None
    
    def _fetch_biodata_samples(self, osd_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch all samples using wildcard endpoint.
        
        Endpoint: /v2/dataset/{OSD-ID}/assay/*/sample/*/?format=json
        
        This returns complete metadata for all assays and samples in one request.
        
        Args:
            osd_id: The OSD identifier
            
        Returns:
            API response as dict, or None on failure
        """
        osd_id = self.normalize_osd_id(osd_id)
        url = f"{self.config.biodata_api_base}/dataset/{osd_id}/assay/*/sample/*/?format=json"
        
        try:
            response = requests.get(url, timeout=self.config.request_timeout)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        
        return None
    
    def _fetch_biodata_assays(self, osd_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch assay list from OSDR Biological Data API.
        
        Endpoint: /v2/dataset/{OSD-ID}/assays/?format=json
        
        Args:
            osd_id: The OSD identifier
            
        Returns:
            API response as dict, or None on failure
        """
        osd_id = self.normalize_osd_id(osd_id)
        url = f"{self.config.biodata_api_base}/dataset/{osd_id}/assays/?format=json"
        
        try:
            response = requests.get(url, timeout=self.config.request_timeout)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        
        return None
    
    # =========================================================================
    # Developer API (Fallback)
    # =========================================================================
    
    def _fetch_developer_metadata(self, osd_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch metadata from OSDR Developer API.
        
        Endpoint: /osd/meta/{numeric_id}
        
        Args:
            osd_id: The OSD identifier
            
        Returns:
            API response as dict, or None on failure
        """
        numeric_id = self.get_numeric_id(osd_id)
        url = f"{self.config.developer_api_base}/osd/meta/{numeric_id}"
        
        try:
            response = requests.get(url, timeout=self.config.request_timeout)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        
        return None
    
    # =========================================================================
    # ISA-Tab Download
    # =========================================================================
    
    def _fetch_files_list(self, osd_id: str) -> Optional[List[Dict[str, Any]]]:
        """Fetch list of files for a study from GeneLab API."""
        try:
            num_id = self.get_numeric_id(osd_id)
            url = f"{self.config.genelab_files_api}/{num_id}"
            
            response = requests.get(url, timeout=self.config.request_timeout)
            
            if response.status_code == 200:
                data = response.json()
                studies = data.get("studies", {})
                osd_id = self.normalize_osd_id(osd_id)
                study_data = studies.get(osd_id, {})
                return study_data.get("study_files", [])
        except Exception:
            pass
        
        return None
    
    def download_isa_tab(self, osd_id: str) -> Optional[Path]:
        """
        Download and extract ISA-Tab archive for a study.
        
        The ISA-Tab archive contains:
        - i_*.txt: Investigation file
        - s_*.txt: Study file (sample characteristics)
        - a_*.txt: Assay file(s)
        
        Args:
            osd_id: The OSD identifier
            
        Returns:
            Path to extracted ISA-Tab directory, or None on failure
        """
        osd_id = self.normalize_osd_id(osd_id)
        extract_dir = self.isa_tab_dir / osd_id
        
        # Check if already downloaded
        if extract_dir.exists() and any(extract_dir.glob("s_*.txt")):
            return extract_dir
        
        # Get files list
        files_list = self._fetch_files_list(osd_id)
        if not files_list:
            return None
        
        # Find ISA zip file
        isa_file = None
        for f in files_list:
            fname = f.get("file_name", "")
            if "ISA.zip" in fname and fname.endswith(".zip"):
                isa_file = f
                break
        
        if not isa_file:
            return None
        
        try:
            # Download the ISA zip
            remote_url = isa_file.get("remote_url", "")
            if remote_url.startswith("/"):
                download_url = f"{self.config.osdr_download_base}{remote_url}"
            else:
                download_url = remote_url
            
            response = requests.get(download_url, timeout=self.config.request_timeout)
            if response.status_code != 200:
                return None
            
            # Extract the zip
            extract_dir.mkdir(parents=True, exist_ok=True)
            zip_buffer = io.BytesIO(response.content)
            
            with zipfile.ZipFile(zip_buffer, "r") as zf:
                zf.extractall(extract_dir)
            
            return extract_dir
            
        except Exception:
            return None
    
    # =========================================================================
    # Response Parsing
    # =========================================================================
    
    def _parse_biodata_response(
        self,
        dataset_data: Optional[Dict[str, Any]],
        samples_data: Optional[Dict[str, Any]],
        assays_data: Optional[Dict[str, Any]],
        osd_id: str,
    ) -> Dict[str, Any]:
        """
        Parse and normalize Biodata API responses into standard format.
        
        Args:
            dataset_data: Response from dataset endpoint
            samples_data: Response from samples wildcard endpoint
            assays_data: Response from assays endpoint
            osd_id: The OSD identifier
            
        Returns:
            Normalized metadata dict with samples, assays, and study info
        """
        osd_id = self.normalize_osd_id(osd_id)
        
        result: Dict[str, Any] = {
            "accession": osd_id,
            "title": "",
            "description": "",
            "organism": "",
            "material_type": "",
            "project_type": "",
            "project_id": "",
            "mission_name": "",
            "assay_types": "",
            "assay_platforms": "",
            "factor_names": "",
            "factor_types": "",
            "protocol_description": "",
            "samples": [],
            "assays": [],
        }
        
        # Parse dataset-level info
        if dataset_data:
            study_data = dataset_data.get("study", {}).get(osd_id, {})
            if not study_data:
                study_data = dataset_data
            
            studies = study_data.get("studies", [])
            if studies:
                first_study = studies[0]
                result["title"] = first_study.get("title", "")
                result["description"] = first_study.get("description", "")
            
            desc_obj = study_data.get("description", {})
            if isinstance(desc_obj, dict):
                result["organism"] = desc_obj.get("organism", "")
                factors = desc_obj.get("factors", [])
                if factors:
                    result["factor_names"] = "     ".join(factors)
        
        # Parse samples
        if samples_data:
            samples_list = []
            samples_obj = None
            
            if "samples" in samples_data:
                samples_obj = samples_data["samples"]
            elif "study" in samples_data:
                study_obj = samples_data.get("study", {}).get(osd_id, {})
                samples_obj = study_obj.get("samples", {})
            
            if samples_obj and isinstance(samples_obj, dict):
                for sample_name, sample_data in samples_obj.items():
                    sample = self._parse_sample_entry(sample_name, sample_data)
                    if sample:
                        samples_list.append(sample)
            elif samples_obj and isinstance(samples_obj, list):
                for sample_data in samples_obj:
                    sample_name = sample_data.get("id", sample_data.get("name", ""))
                    sample = self._parse_sample_entry(sample_name, sample_data)
                    if sample:
                        samples_list.append(sample)
            
            result["samples"] = samples_list
        
        # Parse assays
        if assays_data:
            assays_list = []
            assays_obj = assays_data.get("assays", assays_data)
            
            if isinstance(assays_obj, dict):
                for assay_name, assay_data in assays_obj.items():
                    assay = {
                        "filename": assay_name,
                        "type": assay_data.get("type", ""),
                        "platform": assay_data.get("platform", ""),
                        "sample_count": assay_data.get("sample_count", 0),
                    }
                    assays_list.append(assay)
            elif isinstance(assays_obj, list):
                for assay_data in assays_obj:
                    assay = {
                        "filename": assay_data.get("filename", assay_data.get("name", "")),
                        "type": assay_data.get("type", ""),
                        "platform": assay_data.get("platform", ""),
                        "sample_count": assay_data.get("sample_count", 0),
                    }
                    assays_list.append(assay)
            
            result["assays"] = assays_list
            
            # Build assay_types and platforms strings
            types = set()
            platforms = set()
            for a in assays_list:
                if a.get("type"):
                    types.add(a["type"])
                if a.get("platform"):
                    platforms.add(a["platform"])
            result["assay_types"] = "     ".join(sorted(types))
            result["assay_platforms"] = "     ".join(sorted(platforms))
        
        return result
    
    def _parse_sample_entry(
        self,
        sample_name: str,
        sample_data: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a single sample entry from API response.
        
        Extracts characteristics and factor_values arrays.
        
        Args:
            sample_name: The sample identifier
            sample_data: Raw sample data from API
            
        Returns:
            Normalized sample dict, or None if invalid
        """
        if not sample_name:
            return None
        
        sample: Dict[str, Any] = {
            "id": sample_name,
            "source_name": sample_data.get("source_name", ""),
            "strain": "",
            "sex": "",
            "age": "",
            "material_type": "",
            "characteristics": [],
            "factor_values": {},
        }
        
        # Extract characteristics
        characteristics = sample_data.get("characteristics", [])
        if isinstance(characteristics, list):
            sample["characteristics"] = characteristics
            for char in characteristics:
                category = str(char.get("category", "")).lower()
                value = char.get("value", "")
                
                if "strain" in category:
                    sample["strain"] = value
                elif "sex" in category:
                    sample["sex"] = value
                elif "age" in category:
                    sample["age"] = value
                elif "organism part" in category or "tissue" in category or "material" in category:
                    sample["material_type"] = value
        
        # Check for direct fields (flattened format)
        if not sample["strain"]:
            sample["strain"] = sample_data.get("strain", "")
        if not sample["sex"]:
            sample["sex"] = sample_data.get("sex", "")
        if not sample["age"]:
            sample["age"] = sample_data.get("age", "")
        if not sample["material_type"]:
            sample["material_type"] = sample_data.get("material_type", "")
        
        # Extract factor values
        factor_values = sample_data.get("factorValues", sample_data.get("factor_values", []))
        if isinstance(factor_values, list):
            for fv in factor_values:
                category = fv.get("category", fv.get("name", ""))
                value = fv.get("value", "")
                if category and value:
                    sample["factor_values"][category] = value
        elif isinstance(factor_values, dict):
            sample["factor_values"] = factor_values
        
        return sample
    
    def _parse_developer_response(
        self,
        api_data: Dict[str, Any],
        osd_id: str,
    ) -> Dict[str, Any]:
        """Parse Developer API response into standard format."""
        osd_id = self.normalize_osd_id(osd_id)
        
        result = {
            "accession": api_data.get("Accession", osd_id),
            "title": api_data.get("Study Title", ""),
            "description": api_data.get("Study Description", ""),
            "organism": api_data.get("organism", ""),
            "material_type": api_data.get("Material Type", ""),
            "project_type": api_data.get("Project Type", ""),
            "project_id": api_data.get("Project Identifier", ""),
            "mission_name": "",
            "assay_types": api_data.get("Study Assay Technology Type", ""),
            "assay_platforms": api_data.get("Study Assay Technology Platform", ""),
            "factor_names": api_data.get("Study Factor Name", ""),
            "factor_types": api_data.get("Study Factor Type", ""),
            "protocol_description": api_data.get("Study Protocol Description", ""),
            "samples": [],
            "assays": [],
        }
        
        # Handle mission info
        mission = api_data.get("Mission", {})
        if isinstance(mission, dict):
            result["mission_name"] = mission.get("Name", "")
        elif isinstance(mission, str):
            result["mission_name"] = mission
        
        return result
    
    # =========================================================================
    # Main Entry Point
    # =========================================================================
    
    def fetch_study_json(
        self,
        osd_id: str,
        use_cache: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch complete study metadata including samples from OSDR.
        
        Priority order:
        1. Check local cache (if use_cache=True)
        2. OSDR Biological Data API (PRIMARY)
        3. OSDR Developer API (FALLBACK)
        4. ISA-Tab Parsing (LAST RESORT)
        
        Args:
            osd_id: The OSDR study ID (e.g., "OSD-202" or "202")
            use_cache: If True, check local cache first before API calls
            
        Returns:
            Dictionary containing study metadata and samples, or None if unavailable
        """
        osd_id = self.normalize_osd_id(osd_id)
        
        # Check cache first
        if use_cache:
            cached = self._load_from_cache(osd_id)
            if cached:
                return cached
        
        result = None
        
        # Priority 1: Try OSDR Biological Data API
        dataset_data = self._fetch_biodata_dataset(osd_id)
        samples_data = self._fetch_biodata_samples(osd_id)
        assays_data = self._fetch_biodata_assays(osd_id)
        
        if samples_data or dataset_data:
            result = self._parse_biodata_response(dataset_data, samples_data, assays_data, osd_id)
            
            if result.get("samples"):
                self._save_to_cache(osd_id, result)
                return result
        
        # Priority 2: Try Developer API
        dev_meta = self._fetch_developer_metadata(osd_id)
        
        if dev_meta:
            result = self._parse_developer_response(dev_meta, osd_id)
        
        # Priority 3: Try ISA-Tab for samples
        if not result or not result.get("samples"):
            isa_dir = self.download_isa_tab(osd_id)
            
            if isa_dir:
                # Parse ISA-Tab to extract samples
                isa_metadata = self._isa_parser.parse(osd_id)
                
                if isa_metadata and isa_metadata.samples:
                    # Convert ISA samples to dict format
                    isa_samples = [s.to_dict() for s in isa_metadata.samples]
                    isa_assays = [a.to_dict() for a in isa_metadata.assays]
                    
                    if result:
                        # Merge with existing result
                        result["samples"] = isa_samples
                        result["isa_tab_path"] = str(isa_dir)
                        if not result.get("assays"):
                            result["assays"] = isa_assays
                        if not result.get("title"):
                            result["title"] = isa_metadata.study_title or isa_metadata.investigation_title
                        if not result.get("description"):
                            result["description"] = isa_metadata.study_description or isa_metadata.investigation_description
                        if not result.get("factor_names"):
                            result["factor_names"] = "     ".join(isa_metadata.factors)
                    else:
                        # Create new result from ISA-Tab
                        result = {
                            "accession": osd_id,
                            "title": isa_metadata.study_title or isa_metadata.investigation_title,
                            "description": isa_metadata.study_description or isa_metadata.investigation_description,
                            "isa_tab_path": str(isa_dir),
                            "samples": isa_samples,
                            "assays": isa_assays,
                            "factor_names": "     ".join(isa_metadata.factors),
                        }
        
        # Save to cache if we got samples
        if result and result.get("samples"):
            self._save_to_cache(osd_id, result)
        
        return result

