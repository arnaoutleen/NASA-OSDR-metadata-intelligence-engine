"""
NASA OSDR Metadata Intelligence Engine - Mission Resolver

Maps mission names (e.g., RR-3, MHU-2) to their constituent OSD study IDs
and vice versa. Uses a priority strategy:

1. Local cache (mission_registry.json)
2. Seed registry from constants.KNOWN_MISSIONS
3. ISA-Tab title parsing for dynamic discovery

The resolver caches its mappings to resources/osdr_api/raw/mission_registry.json.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.core.constants import KNOWN_MISSIONS, MISSION_ALIASES
from src.core.osdr_client import OSDRClient


# Patterns for extracting mission names from study titles/descriptions
_MISSION_EXTRACTION_PATTERNS = [
    (r"Rodent Research Reference Mission[- ]?(\d+)", "RRRM-{}"),
    (r"RRRM[- ]?(\d+)", "RRRM-{}"),
    (r"Rodent Research[- ]?(\d+)", "RR-{}"),
    (r"\bRR[- ]?(\d+)\b", "RR-{}"),
    (r"Mouse Habitat Unit[- ]?(\d+)", "MHU-{}"),
    (r"\bMHU[- ]?(\d+)\b", "MHU-{}"),
    (r"\bSTS[- ]?(\d+)\b", "STS-{}"),
    (r"BION[- ]?M?(\d+)", "BION-M{}"),
    (r"SpaceX[- ]?(\d+)", "SpaceX-{}"),
]


def normalize_mission_name(name: str) -> str:
    """Normalize a mission name to its canonical form."""
    lookup = name.lower().strip()
    if lookup in MISSION_ALIASES:
        return MISSION_ALIASES[lookup]
    return name.strip()


class MissionResolver:
    """
    Resolves mission names to OSD study IDs and vice versa.

    The resolver maintains a persistent cache at ``mission_registry.json``
    inside the given cache directory.  On first use it is seeded from
    ``constants.KNOWN_MISSIONS``; subsequent calls to
    :meth:`get_mission_for_osd` extend it dynamically by parsing ISA-Tab
    investigation titles and study descriptions.
    """

    _CACHE_FILENAME = "mission_registry.json"

    def __init__(self, client: OSDRClient, cache_dir: Path):
        self._client = client
        self._cache_path = cache_dir / self._CACHE_FILENAME
        self._registry: Dict[str, List[str]] = {}
        self._load_registry()

    # ------------------------------------------------------------------
    # Registry persistence
    # ------------------------------------------------------------------

    def _load_registry(self) -> None:
        """Load from disk cache, then merge in the seed mapping."""
        if self._cache_path.exists():
            try:
                with open(self._cache_path, "r", encoding="utf-8") as f:
                    self._registry = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._registry = {}

        # Merge seed data (seed never removes entries, only adds missing ones)
        for mission, osds in KNOWN_MISSIONS.items():
            existing = set(self._registry.get(mission, []))
            existing.update(osds)
            self._registry[mission] = sorted(
                existing, key=lambda x: int(x.replace("OSD-", ""))
            )

        self._save_registry()

    def _save_registry(self) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._cache_path, "w", encoding="utf-8") as f:
                json.dump(self._registry, f, indent=2)
        except IOError:
            pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve_mission(self, mission_name: str) -> List[str]:
        """
        Return the list of OSD IDs belonging to a mission.

        Args:
            mission_name: Mission identifier (e.g. ``"RR-3"``).
                          Common aliases are accepted (``"rr-3"``, ``"rr3"``).

        Returns:
            Sorted list of OSD IDs, or empty list if unknown.
        """
        canonical = normalize_mission_name(mission_name)
        return list(self._registry.get(canonical, []))

    def get_mission_for_osd(self, osd_id: str) -> Optional[str]:
        """
        Look up which mission an OSD belongs to.

        Strategy:
        1. Check the in-memory registry
        2. Parse mission from ISA-Tab investigation/study title
        3. Parse mission from developer API study title

        The result is cached so subsequent lookups are instant.

        Args:
            osd_id: OSD identifier (e.g. ``"OSD-242"``)

        Returns:
            Canonical mission name, or ``None`` if unknown.
        """
        osd_id = OSDRClient.normalize_osd_id(osd_id)

        # Check registry
        for mission, osds in self._registry.items():
            if osd_id in osds:
                return mission

        # Try ISA-Tab title parsing
        mission = self._extract_mission_from_isa(osd_id)

        # Fall back to API metadata title/description
        if not mission:
            mission = self._extract_mission_from_api(osd_id)

        if mission:
            existing = set(self._registry.get(mission, []))
            existing.add(osd_id)
            self._registry[mission] = sorted(
                existing, key=lambda x: int(x.replace("OSD-", ""))
            )
            self._save_registry()
            return mission

        return None

    def discover_all_missions(self) -> Dict[str, List[str]]:
        """Return the full mission-to-OSD mapping."""
        return dict(self._registry)

    def list_known_missions(self) -> List[str]:
        """Return sorted list of known mission names."""
        return sorted(self._registry.keys())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _match_mission_in_text(text: str) -> Optional[str]:
        """Extract a mission name from free text using known patterns."""
        for pattern, fmt in _MISSION_EXTRACTION_PATTERNS:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                return fmt.format(m.group(1))
        return None

    def _extract_mission_from_isa(self, osd_id: str) -> Optional[str]:
        """Parse mission from locally available ISA-Tab metadata."""
        from src.core.isa_parser import ISAParser

        parser = ISAParser(self._client.isa_tab_dir)

        # Download if not present
        self._client.download_isa_tab(osd_id)
        metadata = parser.parse(osd_id)
        if not metadata:
            return None

        text = " ".join(filter(None, [
            metadata.investigation_title,
            metadata.study_title,
            metadata.investigation_description,
            metadata.study_description,
        ]))
        return self._match_mission_in_text(text)

    def _extract_mission_from_api(self, osd_id: str) -> Optional[str]:
        """Parse mission from developer API study title/description."""
        dev_meta = self._client._fetch_developer_metadata(osd_id)
        if not dev_meta:
            return None

        study = dev_meta.get("study", {}).get(osd_id, dev_meta.get("study", {}))
        studies = study.get("studies", [])
        if not studies:
            return None

        first = studies[0]
        text = f"{first.get('title', '')} {first.get('description', '')}"
        return self._match_mission_in_text(text)
