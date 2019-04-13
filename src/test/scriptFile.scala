  def splitByParameter(nb: Int): RDD[(String, String)] = {
    loadData()
      .map(_.split(':'))
      .map(x => (x(1),x(6)))
  }