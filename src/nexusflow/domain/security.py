from dataclasses import dataclass

from nexusflow.domain.enums import PrincipalType, PublicPermission


@dataclass(frozen=True, slots=True)
class SecurityContext:
    principal_id: str
    principal_type: PrincipalType
    permissions: frozenset[PublicPermission]

    def has_permission(self, permission: PublicPermission) -> bool:
        return permission in self.permissions
