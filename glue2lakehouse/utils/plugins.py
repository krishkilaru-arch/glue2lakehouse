"""
Plugin system for extending migration capabilities.
Allows custom transformations, validators, and post-processing.
"""

from typing import Dict, Any, List, Callable, Optional
from abc import ABC, abstractmethod
import logging
import importlib
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """Base class for all plugins."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Plugin name."""
        pass
    
    @property
    @abstractmethod
    def version(self) -> str:
        """Plugin version."""
        pass
    
    @abstractmethod
    def initialize(self, config: Dict[str, Any]):
        """
        Initialize plugin with configuration.
        
        Args:
            config: Plugin configuration dictionary
        """
        pass


class TransformPlugin(Plugin):
    """
    Plugin for custom code transformations.
    
    Example:
        ```python
        class MyCustomTransform(TransformPlugin):
            @property
            def name(self):
                return "my_custom_transform"
            
            @property
            def version(self):
                return "1.0.0"
            
            def initialize(self, config):
                self.config = config
            
            def transform(self, code: str, metadata: dict) -> str:
                # Custom transformation logic
                return modified_code
        ```
    """
    
    @abstractmethod
    def transform(self, code: str, metadata: Dict[str, Any]) -> str:
        """
        Apply custom transformation to code.
        
        Args:
            code: Source code string
            metadata: File metadata and analysis results
            
        Returns:
            Transformed code string
        """
        pass


class ValidatorPlugin(Plugin):
    """
    Plugin for custom validation rules.
    """
    
    @abstractmethod
    def validate(
        self,
        source_path: str,
        metadata: Dict[str, Any]
    ) -> tuple[bool, List[str]]:
        """
        Validate source file.
        
        Args:
            source_path: Path to source file
            metadata: File metadata and analysis results
            
        Returns:
            Tuple of (is_valid, list of errors/warnings)
        """
        pass


class PostProcessPlugin(Plugin):
    """
    Plugin for post-migration processing.
    """
    
    @abstractmethod
    def process(
        self,
        source_path: str,
        target_path: str,
        metadata: Dict[str, Any]
    ):
        """
        Post-process migrated file.
        
        Args:
            source_path: Original source file path
            target_path: Migrated target file path
            metadata: Migration metadata
        """
        pass


class HookPlugin(Plugin):
    """
    Plugin for lifecycle hooks.
    """
    
    def on_migration_start(self, source: str, target: str, options: Dict[str, Any]):
        """Called when migration starts."""
        pass
    
    def on_migration_end(self, result: Dict[str, Any]):
        """Called when migration completes."""
        pass
    
    def on_file_start(self, file_path: str):
        """Called when file migration starts."""
        pass
    
    def on_file_end(self, file_path: str, success: bool):
        """Called when file migration completes."""
        pass
    
    def on_error(self, error: Exception, context: Dict[str, Any]):
        """Called when an error occurs."""
        pass


class PluginManager:
    """
    Manages plugin registration, loading, and execution.
    """
    
    def __init__(self):
        self.plugins: Dict[str, Dict[str, Plugin]] = {
            'transform': {},
            'validator': {},
            'post_process': {},
            'hook': {}
        }
        self.logger = logging.getLogger(__name__)
    
    def register_plugin(self, plugin: Plugin, plugin_type: str):
        """
        Register a plugin.
        
        Args:
            plugin: Plugin instance
            plugin_type: Type of plugin ('transform', 'validator', 'post_process', 'hook')
        """
        if plugin_type not in self.plugins:
            raise ValueError(f"Invalid plugin type: {plugin_type}")
        
        plugin_name = plugin.name
        if plugin_name in self.plugins[plugin_type]:
            self.logger.warning(
                f"Plugin '{plugin_name}' already registered. Overwriting."
            )
        
        self.plugins[plugin_type][plugin_name] = plugin
        self.logger.info(f"Registered {plugin_type} plugin: {plugin_name} v{plugin.version}")
    
    def unregister_plugin(self, plugin_name: str, plugin_type: str):
        """
        Unregister a plugin.
        
        Args:
            plugin_name: Name of plugin to remove
            plugin_type: Type of plugin
        """
        if plugin_type in self.plugins and plugin_name in self.plugins[plugin_type]:
            del self.plugins[plugin_type][plugin_name]
            self.logger.info(f"Unregistered {plugin_type} plugin: {plugin_name}")
    
    def get_plugins(self, plugin_type: str) -> List[Plugin]:
        """
        Get all plugins of a specific type.
        
        Args:
            plugin_type: Type of plugin
            
        Returns:
            List of plugin instances
        """
        return list(self.plugins.get(plugin_type, {}).values())
    
    def get_plugin(self, plugin_name: str, plugin_type: str) -> Optional[Plugin]:
        """
        Get a specific plugin.
        
        Args:
            plugin_name: Name of plugin
            plugin_type: Type of plugin
            
        Returns:
            Plugin instance or None
        """
        return self.plugins.get(plugin_type, {}).get(plugin_name)
    
    def apply_transforms(
        self,
        code: str,
        metadata: Dict[str, Any],
        plugin_names: Optional[List[str]] = None
    ) -> str:
        """
        Apply transformation plugins to code.
        
        Args:
            code: Source code
            metadata: File metadata
            plugin_names: Specific plugins to apply (or all if None)
            
        Returns:
            Transformed code
        """
        transforms = self.get_plugins('transform')
        
        if plugin_names:
            transforms = [t for t in transforms if t.name in plugin_names]
        
        for transform in transforms:
            try:
                code = transform.transform(code, metadata)
                self.logger.debug(f"Applied transform: {transform.name}")
            except Exception as e:
                self.logger.error(f"Transform plugin {transform.name} failed: {str(e)}")
        
        return code
    
    def run_validators(
        self,
        source_path: str,
        metadata: Dict[str, Any]
    ) -> tuple[bool, List[str]]:
        """
        Run all validator plugins.
        
        Args:
            source_path: Path to source file
            metadata: File metadata
            
        Returns:
            Tuple of (all_valid, list of all errors/warnings)
        """
        all_valid = True
        all_messages = []
        
        validators = self.get_plugins('validator')
        
        for validator in validators:
            try:
                is_valid, messages = validator.validate(source_path, metadata)
                if not is_valid:
                    all_valid = False
                all_messages.extend(messages)
                self.logger.debug(f"Ran validator: {validator.name}")
            except Exception as e:
                all_valid = False
                all_messages.append(f"Validator plugin {validator.name} failed: {str(e)}")
                self.logger.error(f"Validator plugin {validator.name} failed: {str(e)}")
        
        return all_valid, all_messages
    
    def run_post_processors(
        self,
        source_path: str,
        target_path: str,
        metadata: Dict[str, Any]
    ):
        """
        Run all post-process plugins.
        
        Args:
            source_path: Original source file path
            target_path: Migrated target file path
            metadata: Migration metadata
        """
        processors = self.get_plugins('post_process')
        
        for processor in processors:
            try:
                processor.process(source_path, target_path, metadata)
                self.logger.debug(f"Ran post-processor: {processor.name}")
            except Exception as e:
                self.logger.error(f"Post-processor {processor.name} failed: {str(e)}")
    
    def trigger_hook(self, hook_name: str, *args, **kwargs):
        """
        Trigger a lifecycle hook on all hook plugins.
        
        Args:
            hook_name: Name of hook method to call
            *args: Positional arguments for hook
            **kwargs: Keyword arguments for hook
        """
        hooks = self.get_plugins('hook')
        
        for hook in hooks:
            try:
                method = getattr(hook, hook_name, None)
                if method and callable(method):
                    method(*args, **kwargs)
            except Exception as e:
                self.logger.error(f"Hook {hook_name} in {hook.name} failed: {str(e)}")
    
    def load_plugin_from_file(self, file_path: str, plugin_type: str):
        """
        Load plugin from Python file.
        
        Args:
            file_path: Path to plugin Python file
            plugin_type: Type of plugin
        """
        try:
            # Add file directory to path
            plugin_dir = str(Path(file_path).parent)
            if plugin_dir not in sys.path:
                sys.path.insert(0, plugin_dir)
            
            # Import module
            module_name = Path(file_path).stem
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Find plugin classes
            plugin_base_classes = {
                'transform': TransformPlugin,
                'validator': ValidatorPlugin,
                'post_process': PostProcessPlugin,
                'hook': HookPlugin
            }
            
            base_class = plugin_base_classes.get(plugin_type)
            if not base_class:
                raise ValueError(f"Invalid plugin type: {plugin_type}")
            
            # Instantiate plugins
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (isinstance(attr, type) and 
                    issubclass(attr, base_class) and 
                    attr is not base_class):
                    plugin_instance = attr()
                    plugin_instance.initialize({})
                    self.register_plugin(plugin_instance, plugin_type)
                    self.logger.info(f"Loaded plugin from {file_path}")
        
        except Exception as e:
            self.logger.error(f"Failed to load plugin from {file_path}: {str(e)}")
            raise
    
    def load_plugins_from_directory(self, directory: str):
        """
        Load all plugins from a directory.
        
        Args:
            directory: Directory containing plugin files
        """
        plugin_dir = Path(directory)
        if not plugin_dir.exists():
            self.logger.warning(f"Plugin directory does not exist: {directory}")
            return
        
        for plugin_file in plugin_dir.glob("*.py"):
            if plugin_file.stem.startswith("_"):
                continue
            
            # Try to load as each plugin type
            for plugin_type in ['transform', 'validator', 'post_process', 'hook']:
                try:
                    self.load_plugin_from_file(str(plugin_file), plugin_type)
                except Exception as e:
                    # Continue if loading fails
                    pass
    
    def list_plugins(self) -> Dict[str, List[Dict[str, str]]]:
        """
        List all registered plugins.
        
        Returns:
            Dictionary of plugin types and their plugins
        """
        result = {}
        for plugin_type, plugins in self.plugins.items():
            result[plugin_type] = [
                {'name': p.name, 'version': p.version}
                for p in plugins.values()
            ]
        return result


# Global plugin manager instance
plugin_manager = PluginManager()
