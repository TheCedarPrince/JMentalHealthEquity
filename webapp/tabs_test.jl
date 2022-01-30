using Stipple, StippleUI

@reactive mutable struct Model <: ReactiveModel
    tab_name_1::R{String} = "Tab1"
    tab_name_2::R{String} = "Tab2"
    tab_name_3::R{String} = "Tab3"
end

function ui(my_model)
    page(
        my_model,
        class = "container",
        [
            mtab(
                vmodel = "tab",
                activecolor = "black",
                [
                    tab(label = "FOO"),
                    rtab(label = "BAZ", to = "/"),
                    tab(label = "BAR"),
                ],
            ),
        ],
    )
end

my_model = Stipple.init(Model)

route("/") do
    html(ui(my_model), context = @__MODULE__)
end

route("/hello") do
    "Hello World!"
end
