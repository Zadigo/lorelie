<template>
  <section class="docs">
    <div class="container">
      <div class="row my-5 position-relative">
        <div class="row">
          <div class="col-sm-12 col-md-10 offset-md-1">
            <div class="row">
              <div id="sidebar" class="col-3">
                <aside>
                  <div class="card shadow-sm">
                    <div class="card-body">
                      <div class="list-group">
                        <a href="#database" class="list-group-item">
                          Database
                        </a>
                        
                        <a href="#setting-database" class="list-group-item">
                          Queries
                        </a>
                      </div>
                    </div>
                  </div>
                </aside>
              </div>

              <div class="col-9">
                <RouterView :key="$route.name"></RouterView>
              </div>
              </div>
            </div>
        </div>
      </div>
    </div>
  </section>
</template>

<script>
export default {
  name: 'BaseLayout'
}
</script>

<style scoped>
#sidebar {
  position: sticky;
  top: 0;
  left: 0;
}
</style>
