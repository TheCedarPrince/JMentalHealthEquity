"""
TODO: Add docs when ready
"""
function ExecuteAudit(data::DataFrame; hitech = true)
    df = hitech && filter(row -> row.count >= 11, data)

    return df

end

function ExecuteConnection(;
    host = nothing,
    port = nothing,
    user = nothing,
    password = nothing,
    dialect = :postgresql,
    schema = nothing,
)

    if dialect == :postgresql
        conn = DBInterface.connect(
            LibPQ.Connection,
            "host=$host port=$port user=$user password=$password",
        )
        db_info = reflect(conn; schema = schema, dialect = dialect)

        for key in keys(db_info.tables)
            @eval global $(Symbol(key)) = $(db_info[key])
        end

        return conn
    end

end

# ASSUMPTION: The Database follows the OMOP CDM 5.4 Schema completely

export ExecuteAudit, ExecuteConnection
