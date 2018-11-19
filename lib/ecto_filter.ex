defmodule EctoFilter do
  import Ecto.Query, warn: false
  @apidoc """
  @apiDescription 查询-排序-分页
  @api {get} rummage  查询-排序-分页
  @apiVersion 0.0.1
  @apiGroup rummage
  @apiName rummage
  @apiSampleRequest off
  @apiParam {Object} [search] 刷选参数:
  @apiParam {Object} search.field1 `field1`和`field1_value`特殊值说明：`is_null:未空|all:全部`（使用参照下列示例）
  @apiParam {Object} search.scope 时间范围
  @apiParam {Date} search.scope.startTime 时间开始，格式："2017-1-1"
  @apiParam {Date} search.scope.endTime 时间结束，格式："2017-12-1"
  @apiParam {Number} search.interval 时间段 <br>
    情况1：`0:今日|-6：最近7天|-29：最近30天`  <br>
    情况2： `-1:昨日|-7：过去7天|-30：过去30天`
  @apiParam {Number} search.tz 时区差（秒），例如,东八区：tz=28800 西八区：tz=-28800
  @apiParam {Object} [sort] 排序参数(默认按记录id升序排序)-尚未实现:
  @apiParam {Object} sort.field1 `field1`是具体字段名，具体参考对应接口说明
  @apiParam {String} sort.field1.order 排序方式 asc|desc
  @apiParam {Object} paginate 分页参数:
  @apiParam {Number} paginate.page=1 所选页码数.
  @apiParam {Number} [paginate.page_size=20]  每页条目数
  @apiParamExample {json} 分页示例:
  // 每次最多显示1行为限制，获取第1页的留言记录
    {paginate: {page: 1,page_size: 1}}
  @apiParamExample {json} 查询-排序-分页示例
  // 获取昨天class=财务的留言记录： 以class=财务为刷选条件，按留言时间降序排序，每次最多显示20行为限制
  // 从下面示例可以看出：这时的field1 = class
  // 这时的field1_value = "财务"
  {
    search: {class: "财务",interval: -2},
    sort: {inserted_at: %{order: desc}},
    paginate: {page: 2,page_size: 20}
  }

  // 获取2017-1-1至2017-12-1未分类的留言记录：以class=is_null为刷选条件，按留言时间降序排序，每次最多显示20行为限制
  {
    search: {class: "is_null",scope: {startTime: "2017-1-1",endTime: "2017-12-1"}},
    sort: {inserted_at: %{order: desc}},
    paginate: {page: 2,page_size: 20}
  }
  @apiParamExample {text} URL参数示例:
    ?paginate[page]=1&paginate[page_size]=1&search[class]=is_null&sort[class][order]=asc
  @apiSuccess {Object[]}  data 返回的数据，具体查看实际返回
  @apiHeader (Response Headers) {String} link 链接集
  @apiHeader (Response Headers) {Number} total 总共条目数
  @apiHeader (Response Headers) {Number} per-page 每页条目数
  @apiHeader (Response Headers) {Number} total_pages 总共页数
  @apiHeader (Response Headers) {Number} page-number 当前所选页码数
  @apiHeaderExample {text} Response Hearder Example:
  link: <http://localhost:4002/authed/leaves?page=2&paginate%5Bpage%5D=1&paginate%5Bpage_size%5D=1&rummage=&search=&search.class=&search.inserted_at=&search.state=&search%5Bclass%5D=is_null>; rel="next", <http://localhost:4002/authed/leaves?page=1&paginate%5Bpage%5D=1&paginate%5Bpage_size%5D=1&rummage=&search=&search.class=&search.inserted_at=&search.state=&search%5Bclass%5D=is_null>; rel="first", <http://localhost:4002/authed/leaves?page=2&paginate%5Bpage%5D=1&paginate%5Bpage_size%5D=1&rummage=&search=&search.class=&search.inserted_at=&search.state=&search%5Bclass%5D=is_null>; rel="last"
  total: 2
  per-page: 1
  total-pages: 2
  page-number: 1
  """
  @doc false
  def apidoc_example(), do: @apidoc

  # 过滤分类
  def filter_class(query, %{"class" => "is_null"}) do
    query
    |> where([t], is_nil(t.class))
  end
  def filter_class(query, %{"class" => "all"}), do: query
  def filter_class(query, %{"class" => val}) do
    query
    |> where([t], t.class == ^val)
  end
  def filter_class(query, %{"search" => search}), do: filter_class(query, search)
  def filter_class(query, _), do: query


  # 过滤状态
  def filter_state(query, %{"state" => "is_null"}) do
    query
    |> where([t], is_nil(t.state))
  end
  def filter_state(query, %{"state" => "all"}), do: query
  def filter_state(query, %{"state" => val}) do
    val = String.to_integer(val)
    query
    |> where([t], t.state == ^val)
  end
  def filter_state(query, %{"search" => search}), do: filter_state(query, search)
  def filter_state(query, _), do: query


  # 过滤客服编号
  def filter_wid(query, %{"wid" => "is_null"}) do
    query
    |> where([t], is_nil(t.wid))
  end
  def filter_wid(query, %{"wid" => "all"}), do: query
  def filter_wid(query, %{"wid" => val}) when is_nil(val), do: query
  def filter_wid(query, %{"wid" => val}) do
    val = if is_integer(val), do: val, else: String.to_integer(val)
    query
    |> where([t], t.wid == ^val)
  end
  def filter_wid(query, %{"search" => search}), do: filter_wid(query, search)
  def filter_wid(query, _), do: query


  # 过滤分类
  def filter_class_assoc(query, %{"class" => "is_null"}, _, assoc) do
    query
    |> join(:left, [t], c in assoc(t, ^assoc))
    |> where([t,c], is_nil(c.class))
  end
  def filter_class_assoc(query, %{"class" => "all"}, _, _), do: query
  def filter_class_assoc(query, %{"class" => val}, join_type, assoc) do
    query
    |> join(join_type, [t], c in assoc(t, ^assoc), on: c.class == ^val)
  end
  def filter_class_assoc(query, %{"search" => search}, join_type, assoc),
      do: filter_class_assoc(query, search, join_type, assoc)
  def filter_class_assoc(query, _, _, _), do: query

  # 过滤分类
  def filter_state_assoc(query, %{"state" => "is_null"}, _, assoc) do
    query
    |> join(:left, [t], c in assoc(t, ^assoc))
    |> where([t,c], is_nil(c.state))
  end
  def filter_state_assoc(query, %{"state" => "all"}, _, _), do: query
  def filter_state_assoc(query, %{"state" => val}, join_type, assoc) do
    query
    |> join(join_type, [t], c in assoc(t, ^assoc), on: c.state == ^val)
  end
  def filter_state_assoc(query, %{"search" => search}, join_type, assoc),
      do: filter_state_assoc(query, search, join_type, assoc)
  def filter_state_assoc(query, _, _, _), do: query

  # 过滤分类
  def filter_com_state_assoc(query, %{"com_state" => "is_null"}, _, assoc) do
    query
    |> join(:left, [t], c in assoc(t, ^assoc))
    |> where([t,c], is_nil(c.state))
  end
  def filter_com_state_assoc(query, %{"com_state" => "all"}, _, _), do: query
  def filter_com_state_assoc(query, %{"com_state" => val}, join_type, assoc) do
    query
    |> join(join_type, [t], c in assoc(t, ^assoc), on: c.state == ^val)
  end
  def filter_com_state_assoc(query, %{"search" => search}, join_type, assoc),
      do: filter_com_state_assoc(query, search, join_type, assoc)
  def filter_com_state_assoc(query, _, _, _), do: query


  # 过滤分类
  def filter_smr_state_assoc(query, %{"smr_state" => "is_null"}, _, assoc) do
    query
    |> join(:left, [t], c in assoc(t, ^assoc))
    |> where([t,c], is_nil(c.state))
  end
  def filter_smr_state_assoc(query, %{"smr_state" => "all"}, _, _), do: query
  def filter_smr_state_assoc(query, %{"smr_state" => val}, join_type, assoc) do
    query
    |> join(join_type, [t], c in assoc(t, ^assoc), on: c.state == ^val)
  end
  def filter_smr_state_assoc(query, %{"search" => search}, join_type, assoc),
      do: filter_smr_state_assoc(query, search, join_type, assoc)
  def filter_smr_state_assoc(query, _, _, _), do: query


  # 过滤分类
  def filter_by_assoc(query, %{"state" => "is_null"}, _, assoc) do
    query
    |> join(:left, [t], c in assoc(t, ^assoc))
    |> where([t,c], is_nil(c.state))
  end
  def filter_by_assoc(query, %{"state" => "all"}, _, _), do: query
  def filter_by_assoc(query, %{"state" => val}, join_type, assoc) do
    query
    |> join(join_type, [t], c in assoc(t, ^assoc), on: c.state == ^val)
  end
  def filter_by_assoc(query, %{"search" => search}, join_type, assoc),
      do: filter_by_assoc(query, search, join_type, assoc)
  def filter_by_assoc(query, _, _, _), do: query

  #过滤快捷回复id
  def filter_fastreply_id(query, %{"fastreply_id" => search}) do
    query
    |> where([t], t.fastreply_id == ^search)
  end

  #过滤公告搜索
  def filter_anno_id(query, %{"search" => search}) do
    query
    |> where([t], like(t.sender, ^search) or like(t.receiver, ^search) or like(t.title, ^search))
  end

  #过滤公告显示搜索分类
  def filter_anno_class(query, %{"search" => search}) do
    case search == "" do
      true ->
        query
      _ ->
        query
        |> where([t], t.is_read == ^search)
    end
  end


  @doc """
  过滤时间-今日
  tz: 时区差（秒），例如,东八区：tz=28800 西八区：tz=-28800
  """
  def filter_time_in_today(query, tz),
      do: filter_time_from_today(query, %{interval: 0, tz: tz, now: NaiveDateTime.utc_now()})

  @doc """
  过滤时间
  interval: 0:今日|-6：最近7天|-29：最近30天
  tz: 时区差（秒），例如,东八区：tz=28800 西八区：tz=-28800
  startTime: NaiveDtateTime.t
  endTime: NaiveDtateTime.t
  ## Examples

      iex> EctoFilter.filter_time_from_today(ImWebserver.Leaves.Leave,%{interval: 0,tz: 0,now: NaiveDateTime.utc_now})
      iex> EctoFilter.filter_time_from_today(ImWebserver.Leaves.Leave,%{startTime: ~N[2018-08-28 00:00:00],endTime: ~N[2018-08-28 23:59:59],tz: 0,now: NaiveDateTime.utc_now})
      %Ecto.Query{}
  """
  def filter_time_from_today(query, nil), do: query
  def filter_time_from_today(query, %{interval: interval, tz: tz, now: now}) when interval < 1 do
    tz_begin = Timex.beginning_of_day(now)
               |> Timex.shift(seconds: -tz)
               |> Timex.shift(days: interval)
    tz_end = Timex.end_of_day(now)
             |> Timex.shift(seconds: -tz)

    query
    |> where([t], t.inserted_at >= ^tz_begin and t.inserted_at <= ^tz_end)
  end

  def filter_time_from_today(query, %{startTime: startTime, endTime: endTime, tz: tz, now: now}) do
    tz_begin = startTime
               |> Timex.shift(seconds: -tz)
    today_end = Timex.end_of_day(now)
                |> Timex.shift(seconds: tz)
    tz_end = case NaiveDateTime.compare(endTime, today_end)  do
               :lt -> endTime
               _ -> today_end
             end
             |> Timex.shift(seconds: -tz)
    query
    |> where([t], t.inserted_at >= ^tz_begin and t.inserted_at <= ^tz_end)
  end
  def filter_time_from_today(query, %{"search" => search}),
      do: filter_time_from_today(query, format_search_time(search))
  def filter_time_from_today(query, %{"paginate" => _}), do: query
  @doc """
  过滤时间
  interval: -1:昨日|-7：过去7天|-30：过去30天
  tz: 时区差（秒），例如,东八区：tz=28800 西八区：tz=-28800
  startTime: NaiveDtateTime.t
  endTime: NaiveDtateTime.t
  ## Examples

      iex> EctoFilter.filter_time_from_yesterday(ImWebserver.Leaves.Leave,%{interval: -1,tz: 0,now: NaiveDateTime.utc_now})
          EctoFilter.filter_time_from_yesterday(ImWebserver.Leaves.Leave,%{startTime: ~N[2018-08-27 00:00:00],endTime: ~N[2018-08-27 23:59:59],tz: 0,now: NaiveDateTime.utc_now})
      %Ecto.Query{}
  """
  def filter_time_from_yesterday(query, nil), do: query
  def filter_time_from_yesterday(query, %{interval: interval, tz: tz, now: now}) when interval < 0 do
    tz_begin = Timex.beginning_of_day(now)
               |> Timex.shift(seconds: -tz)
               |> Timex.shift(days: interval)
    tz_end = Timex.end_of_day(now)
             |> Timex.shift(days: -1)
             |> Timex.shift(seconds: -tz)
    query
    |> where([t], t.inserted_at >= ^tz_begin and t.inserted_at <= ^tz_end)
  end

  def filter_time_from_yesterday(query, %{startTime: startTime, endTime: endTime, tz: tz, now: now}) do
    tz_begin = startTime
               |> Timex.shift(seconds: -tz)
    yesterday_end = Timex.beginning_of_day(now)
                    |> Timex.shift(seconds: tz)
    tz_end = case NaiveDateTime.compare(endTime, yesterday_end)  do
               :lt -> endTime
               _ -> yesterday_end
             end
             |> Timex.shift(seconds: -tz)
    query
    |> where([t], t.inserted_at >= ^tz_begin and t.inserted_at <= ^tz_end)
  end
  def filter_time_from_yesterday(query, %{"search" => search}),
      do: filter_time_from_yesterday(query, format_search_time(search))
  def filter_time_from_yesterday(query, %{"paginate" => _}), do: query

  def format_search_time(search) do
    args = case search["tz"] do
      nil ->
        %{tz: Timex.local.utc_offset, now: NaiveDateTime.utc_now}
      tz ->
        tz = if is_binary(tz), do: String.to_integer(tz), else: tz
        %{tz: tz, now: NaiveDateTime.utc_now}
    end
    cond do
      search["interval"] == "all" ->
        nil
      search["interval"] ->
        interval = if is_binary(search["interval"]), do: String.to_integer(search["interval"]), else: search["interval"]
        %{interval: interval}
        |> Map.merge(args)
      search["startTime"] ->
        startTime = NaiveDateTime.from_iso8601!(search["startTime"] <> " 00:00:00")
                    |> Timex.beginning_of_day()
        endTime = NaiveDateTime.from_iso8601!(search["endTime"] <> " 00:00:00")
                  |> Timex.end_of_day()
        %{startTime: startTime, endTime: endTime}
        |> Map.merge(args)
      true ->
        nil
    end
  end

  #TODO need add assoc in the future
  def sort(query, %{"sort" => sort}) do
    Enum.reduce(
      sort,
      query,
      fn {k, v}, acc ->
        acc
        |> order_by([{^String.to_atom(v["order"]), ^String.to_atom(k)}])
      end
    )
  end
  def sort(query, %{}), do: query



  #TODO 动态关联查询以后完善
  #  dy = %{
  #         "search" => %{
  #           "class" => %{
  #             "search_spec" => "and",
  #             "search_type" => "is_null",
  #             "search_term" => "true"
  #           },
  #           "state" => %{
  #             "search_spec" => "and",
  #             "search_type" => "eq",
  #             "search_term" => 1
  #           }
  #         }
  #       }
  #       |> gen_dynamics()
  def gen_dynamics(%{"search" => search}) do
    f = fn {k, x}, acc ->
      search_field = format_search_field(k)
      search_spec = format_search_spec(x["search_spec"])
      search_type = format_search_type(x["search_type"])
      search_term = format_search_term(x["search_term"])
      gen_dynamic(acc, search_spec, search_field, search_type, search_term)
    end
    Enum.reduce(search, true, f)
  end
  def gen_dynamic(dy, :and, field, :eq, search_term), do: dynamic([t, c], field(c, ^field) == ^search_term and ^dy)
  def gen_dynamic(dy, :and, field, :is_null, true), do: dynamic([t, c], is_nil(field(c, ^field)) and ^dy)
  def gen_dynamic(dy, :and, field, :is_null, false), do: dynamic([t, c], not is_nil(field(c, ^field)) and ^dy)
  def gen_dynamic(dy, :and, _, :all, _), do: dy
  def gen_dynamic(dy, :or, field, :eq, search_term), do: dynamic([t, c], field(c, ^field) == ^search_term or ^dy)
  def gen_dynamic(dy, _, _, _, _), do: dy

  def format_search_spec(search_spec) when is_binary(search_spec), do: String.to_atom(search_spec)
  def format_search_spec(search_spec), do: String.to_atom(search_spec)

  def format_search_field(search_field) when is_binary(search_field), do: String.to_atom(search_field)
  def format_search_field(search_field), do: String.to_atom(search_field)

  def format_search_type(search_type) when is_binary(search_type), do: String.to_atom(search_type)
  def format_search_type(search_type), do: String.to_atom(search_type)

  def format_search_term("true"), do: true
  def format_search_term("false"), do: false
  def format_search_term(search_term), do: search_term


  # 格式化interval、startTime参数
  def convert_to_new_search_params(%{"interval" => interval} = params, tz) do
    cond do
      interval == "all" ->
        Map.delete(params, "interval")
      true ->
        interval = if is_binary(interval), do: String.to_integer(interval), else: interval
        params
        |> Map.put("interval", interval + 1)
    end
    |> Map.put("tz", tz)
    |> format_search_time()

  end

  def convert_to_new_search_params(%{"startTime" => _} = params, tz) do
    params
    |> Map.put("tz", tz)
    |> format_search_time()

  end

  def convert_to_new_search_params(params, tz) do
    Map.put(params, "interval", "all")
    |> convert_to_new_search_params(tz)
  end


  #数据按小时分组
  def patch_by_hours([], _, acc), do: acc
  def patch_by_hours([rec | reset], tz, acc) do
    hour = Timex.shift(rec.inserted_at, seconds: tz)
           |> Map.get(:hour)
    acc = acc
          |> Map.put(hour, (acc[hour] || 0) + 1)
          |> Map.put("total", (acc["total"] || 0) + 1)
    patch_by_hours(reset, tz, acc)
  end

  #数据按天分组
  def patch_by_days([], _, acc), do: acc
  def patch_by_days([rec | reset], tz, acc) do
    date = Timex.shift(rec.inserted_at, seconds: tz)
           |> Timex.format!("{YYYY}-{0M}-{0D}")
    acc = acc
          |> Map.put(date, (acc[date] || 0) + 1)
          |> Map.put("total", (acc["total"] || 0) + 1)
    patch_by_days(reset, tz, acc)
  end

  #根据参数生成日期列表
  def gen_date_indexs_by_search_time(nil), do: []
  def gen_date_indexs_by_search_time(%{startTime: startTime, endTime: endTime})  do
    gen_date_indexs_by_dateTime(startTime, endTime)
  end
  def gen_date_indexs_by_search_time(%{interval: interval, tz: tz, now: now})  do
    gen_date_indexs_by_interval(interval, tz, now)
  end

  #根据日期参数生成日期列表
  def gen_date_indexs_by_dateTime(startTime, endTime)  do
    startDate = startTime
                |> NaiveDateTime.to_date()
    interval = endTime
               |> NaiveDateTime.to_date()
               |> Date.diff(startDate)
    startDate
    |> gen_date_indexs(interval)
  end

  #根据区间参数生成日期列表
  def gen_date_indexs_by_interval(interval, tz, utc_now)  do
    Timex.beginning_of_day(utc_now)
    |> Timex.shift(seconds: tz)
    |> NaiveDateTime.to_date()
    |> gen_date_indexs(interval)
  end


  #根据具体日期生成日期列表
  def gen_date_indexs(startDate, i) when i >= 0 do
    shif_date = fn (i) ->
      Timex.shift(startDate, days: i)
      |> Date.to_string()
    end
    Enum.map(0..i, shif_date)
  end
  def gen_date_indexs(startDate, i) when i < 0 do
    shif_date = fn (i) ->
      Timex.shift(startDate, days: i)
      |> Date.to_string()
    end
    Enum.map(i..0, shif_date)
  end

end
