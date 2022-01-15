using Stipple, StippleUI

@reactive mutable struct Model <: ReactiveModel
  valone::R{Bool} = false
  valtwo::R{Bool} = false
  valthree::R{Bool} = false
  fruits::R{Vector} = []
end

function handler(model)
	on(model.valone) do val
		if val == true
			push!(model.fruits.o.val, "Apples")
		else
			deleteat!(model.fruits.o.val, findall(x -> x == "Apples", model.fruits.o.val))
		end
	end
	on(model.valtwo) do val
		if val == true
			push!(model.fruits.o.val, "Bananas")
		else
			deleteat!(model.fruits.o.val, findall(x -> x == "Bananas", model.fruits.o.val))
		end
	end
	on(model.valthree) do val
		if val == true
			push!(model.fruits.o.val, "Mangos")
		else
			deleteat!(model.fruits.o.val, findall(x -> x == "Mangos", model.fruits.o.val))
		end
	end
	model
end


function ui(my_model)
  page(
    my_model, class="container", [
      checkbox(label = "Apples", fieldname = :valone, dense = true),
      checkbox(label = "Bananas", fieldname = :valtwo, dense = true),
      checkbox(label = "Mangos", fieldname = :valthree, dense = true),
    ],
  )
end

my_model = handler(Stipple.init(Model))

route("/") do 
  html(ui(my_model), context = @__MODULE__)
end
