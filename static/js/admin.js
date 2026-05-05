(function () {
  const tableWrappers = document.querySelectorAll(".table-wrap");
  tableWrappers.forEach((node) => {
    node.setAttribute("tabindex", "0");
  });
})();

