from __future__ import annotations

class PluginRegistry:
    def __init__(self) -> None:
        self._instances: dict[str, object] = {}

    def register_instance(self, mp: str, inst: object) -> None:
        self._instances[mp] = inst

    def get_instance(self, mp: str) -> object | None:
        return self._instances.get(mp)

    def remove_plugin(self, mp: str) -> None:
        _ = self._instances.pop(mp, None)

    def remove_module_tree(self, root: str) -> None:
        module_paths = {
            root,
            *(path for path in self._instances if path.startswith(f"{root}.")),
        }
        for module_path in module_paths:
            self.remove_plugin(module_path)


plugin_registry = PluginRegistry()
