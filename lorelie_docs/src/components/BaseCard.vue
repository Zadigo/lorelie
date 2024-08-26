<template>
  <div :id="cardId" class="card shadow-sm">
    <slot name="body">
      <div class="card-body">
        <h3 v-if="title" class="card-title">
          {{ title }}
        </h3>
        
        <slot class="fw-light"></slot>
      </div>
    </slot>
  </div>
</template>

<script>
export default {
  name: 'BaseCard',
  props: {
    title: {
      type: String,
      default: null
    },
    cardId: {
      type: String,
      required: true
    }
  }
}
</script>
