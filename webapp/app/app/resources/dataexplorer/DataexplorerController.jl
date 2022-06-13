module DataexplorerController

using Genie, Stipple, StippleUI, StipplePlotly
using Genie.Renderers.Html
using Dataexplorer

function explorer()
    html(:dataexplorer, "dataexplorer.jl", model = Dataexplorer.model_factory(), context = @__MODULE__)
end

end
