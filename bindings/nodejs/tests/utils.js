export function generateBytes() {
  const size = Math.floor(Math.random() * 1024) + 1
  const content = []

  for (let i = 0; i < size; i++) {
    content.push(Math.floor(Math.random() * 256))
  }

  return Buffer.from(content)
}
