using System;
using System.Collections.Generic;
using System.Linq;
using DbSync.Core.Providers;

namespace DbSync.Cli;

public sealed class ProviderRegistry
{
    private readonly IReadOnlyDictionary<ProviderKind, IProvider> _providersByKind;
    private readonly IReadOnlyDictionary<string, IProvider> _providersByName;

    public ProviderRegistry(IEnumerable<IProvider> providers)
    {
        if (providers is null) throw new ArgumentNullException(nameof(providers));

        var providerList = providers.ToList();
        _providersByKind = providerList.ToDictionary(p => p.Kind);
        _providersByName = providerList.ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);
    }

    public IEnumerable<IProvider> Providers => _providersByKind.Values;

    public IProvider GetByKind(ProviderKind kind)
    {
        return _providersByKind[kind];
    }

    public IProvider? TryGetByName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        return _providersByName.TryGetValue(name, out var provider) ? provider : null;
    }
}
